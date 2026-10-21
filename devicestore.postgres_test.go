// File: devicestore.postgres_test.go

package grnoti

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testPostgresDSN = "host=localhost user=postgres_user password=postgres_password dbname=grnoti_test port=5432 sslmode=disable"

// ensureTestPostgresSchema applies grnoti's Postgres schema against
// testPostgresDSN. None of the four NewPostgres* constructors apply
// schema themselves anymore (see connectPostgres's own doc comment in
// postgres.go) -- every one of grnoti's own Postgres test helpers below,
// and every "Postgres" contract-test subtest, calls this first so the
// suite stays self-contained against a fresh database without needing a
// real migration tool. Safe to call repeatedly and from multiple test
// files: applyPostgresSchema is CREATE TABLE/INDEX IF NOT EXISTS,
// serialized by its own Postgres advisory lock.
func ensureTestPostgresSchema(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testPostgresDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	if err := applyPostgresSchema(ctx, pool); err != nil {
		t.Fatalf("applyPostgresSchema: %v", err)
	}
}

func newTestPostgresDeviceStore(t *testing.T) DeviceStore {
	t.Helper()
	ensureTestPostgresSchema(t)
	store, err := NewPostgresDeviceStore(PostgresConfig{DSN: testPostgresDSN})
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	t.Cleanup(func() {
		if s, ok := store.(*postgresDeviceStore); ok {
			s.pool.Exec(context.Background(), "DELETE FROM grnoti_devices")
		}
		_ = store.Close()
	})
	return store
}

func TestNewPostgresDeviceStore_ConnectError(t *testing.T) {
	if _, err := NewPostgresDeviceStore(PostgresConfig{}); err == nil {
		t.Fatal("NewPostgresDeviceStore(empty DSN) = nil error, want non-nil")
	}
}

func TestPostgresDeviceStore_SaveAndGet(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	ctx := context.Background()

	if err := store.SaveDevice(ctx, Device{FID: testFID("pgt1"), UserID: "pgu1", Platform: PlatformIOS}); err != nil {
		t.Fatalf("SaveDevice: %v", err)
	}
	devices, err := store.GetActiveDevices(ctx, "pgu1")
	if err != nil || len(devices) != 1 || devices[0].FID != testFID("pgt1") {
		t.Fatalf("GetActiveDevices() = (%v, %v), want [pgt1]", devices, err)
	}
}

func TestPostgresDeviceStore_SaveDevice_UpsertReactivates(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	ctx := context.Background()

	_ = store.SaveDevice(ctx, Device{FID: testFID("pgt2"), UserID: "pgu2"})
	_ = store.MarkInvalid(ctx, testFID("pgt2"))
	if devices, _ := store.GetActiveDevices(ctx, "pgu2"); len(devices) != 0 {
		t.Fatalf("expected inactive after MarkInvalid, got %v", devices)
	}
	_ = store.SaveDevice(ctx, Device{FID: testFID("pgt2"), UserID: "pgu2"})
	if devices, _ := store.GetActiveDevices(ctx, "pgu2"); len(devices) != 1 {
		t.Fatalf("expected reactivated after re-save, got %v", devices)
	}
}

func TestPostgresDeviceStore_GetActiveDevicesByAnonymousID(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	ctx := context.Background()
	_ = store.SaveDevice(ctx, Device{FID: testFID("pgt3"), AnonymousID: "pga1"})

	devices, err := store.GetActiveDevicesByAnonymousID(ctx, "pga1")
	if err != nil || len(devices) != 1 {
		t.Fatalf("GetActiveDevicesByAnonymousID() = (%v, %v), want 1 device", devices, err)
	}
}

func TestPostgresDeviceStore_GetActiveDevicesBatch(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	ctx := context.Background()
	_ = store.SaveDevice(ctx, Device{FID: testFID("pgt4"), UserID: "pgu4"})
	_ = store.SaveDevice(ctx, Device{FID: testFID("pgt5"), UserID: "pgu5"})

	out, err := store.GetActiveDevicesBatch(ctx, []string{"pgu4", "pgu5", "pgu6"})
	if err != nil || len(out) != 2 {
		t.Fatalf("GetActiveDevicesBatch() = (%v, %v), want 2 users", out, err)
	}
}

func TestPostgresDeviceStore_DeleteDevice(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	ctx := context.Background()
	_ = store.SaveDevice(ctx, Device{FID: testFID("pgt6"), UserID: "pgu6"})

	if err := store.DeleteDevice(ctx, testFID("pgt6")); err != nil {
		t.Fatalf("DeleteDevice: %v", err)
	}
	if devices, _ := store.GetActiveDevices(ctx, "pgu6"); len(devices) != 0 {
		t.Fatalf("device still present: %v", devices)
	}
	if err := store.DeleteDevice(ctx, testFID("pgt6")); err != nil {
		t.Fatalf("DeleteDevice (already deleted): %v", err)
	}
}

func TestPostgresDeviceStore_Close_Idempotent(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
	if _, err := store.GetActiveDevices(context.Background(), "pgu1"); err != ErrClosed {
		t.Fatalf("GetActiveDevices after Close error = %v, want ErrClosed", err)
	}
}

// TestPostgresDeviceStore_GenericQueryError uses an already-canceled
// context to force a real query-level error from every method — the
// generic (non-ErrClosed) "backend unavailable" wrap branch each method
// has, otherwise unreachable against a healthy database without fault
// injection.
func TestPostgresDeviceStore_GenericQueryError(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.SaveDevice(ctx, Device{FID: testFID("t1"), UserID: "u1"}); err == nil {
		t.Error("SaveDevice(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.GetActiveDevices(ctx, "u1"); err == nil {
		t.Error("GetActiveDevices(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.GetActiveDevicesByAnonymousID(ctx, "a1"); err == nil {
		t.Error("GetActiveDevicesByAnonymousID(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.GetActiveDevicesBatch(ctx, []string{"u1"}); err == nil {
		t.Error("GetActiveDevicesBatch(canceled ctx) = nil error, want non-nil")
	}
	if err := store.MarkInvalid(ctx, testFID("t1")); err == nil {
		t.Error("MarkInvalid(canceled ctx) = nil error, want non-nil")
	}
	if err := store.DeleteDevice(ctx, testFID("t1")); err == nil {
		t.Error("DeleteDevice(canceled ctx) = nil error, want non-nil")
	}
}

// TestPostgresStores_SharedPool_CloseDoesNotAffectSiblingStore is the
// regression test for the ownership fix: two stores built from one
// injected *pgxpool.Pool (PostgresConfig.Pool) must not close that pool
// out from under each other — only a store that dialed its own pool from
// DSN should ever close it.
func TestPostgresStores_SharedPool_CloseDoesNotAffectSiblingStore(t *testing.T) {
	ensureTestPostgresSchema(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, testPostgresDSN)
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}

	deviceStore, err := NewPostgresDeviceStore(PostgresConfig{Pool: pool})
	if err != nil {
		t.Fatalf("NewPostgresDeviceStore(Pool: pool): %v", err)
	}
	preferencesStore, err := NewPostgresPreferencesStore(PostgresConfig{Pool: pool})
	if err != nil {
		t.Fatalf("NewPostgresPreferencesStore(Pool: pool): %v", err)
	}
	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM grnoti_devices")
		pool.Exec(ctx, "DELETE FROM grnoti_preferences")
	})

	if err := deviceStore.Close(); err != nil {
		t.Fatalf("deviceStore.Close(): %v", err)
	}

	// The shared pool, and the sibling store still using it, must remain
	// usable after deviceStore.Close() — proving Close() didn't close the
	// pool it doesn't own.
	if err := preferencesStore.SavePreferences(ctx, &NotificationPreferences{UserID: "shared-pool-user", GlobalEnabled: true}); err != nil {
		t.Fatalf("SavePreferences after sibling store's Close(): %v", err)
	}
	if _, err := preferencesStore.GetPreferences(ctx, "shared-pool-user"); err != nil {
		t.Fatalf("GetPreferences after sibling store's Close(): %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("pool.Ping() after sibling store's Close(): %v, want the shared pool to still be open", err)
	}

	_ = preferencesStore.Close()
}

func TestPostgresDeviceStore_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	store := newTestPostgresDeviceStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if err := store.SaveDevice(ctx, Device{FID: testFID("t1"), UserID: "u1"}); err != ErrClosed {
		t.Errorf("SaveDevice after Close = %v, want ErrClosed", err)
	}
	if _, err := store.GetActiveDevicesByAnonymousID(ctx, "a1"); err != ErrClosed {
		t.Errorf("GetActiveDevicesByAnonymousID after Close = %v, want ErrClosed", err)
	}
	if _, err := store.GetActiveDevicesBatch(ctx, []string{"u1"}); err != ErrClosed {
		t.Errorf("GetActiveDevicesBatch after Close = %v, want ErrClosed", err)
	}
	if err := store.MarkInvalid(ctx, testFID("t1")); err != ErrClosed {
		t.Errorf("MarkInvalid after Close = %v, want ErrClosed", err)
	}
	if err := store.DeleteDevice(ctx, testFID("t1")); err != ErrClosed {
		t.Errorf("DeleteDevice after Close = %v, want ErrClosed", err)
	}
}
