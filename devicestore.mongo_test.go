// File: devicestore.mongo_test.go

package grnoti

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

const testMongoURI = "mongodb://root:mongo_password@localhost:27018/?directConnection=true"

func newTestMongoDeviceStore(t *testing.T) DeviceStore {
	t.Helper()
	store, err := NewMongoDeviceStore(MongoDeviceStoreConfig{
		URI: testMongoURI, Database: "grnoti_test", CollectionName: fmt.Sprintf("devices_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestNewMongoDeviceStore_EmptyURI(t *testing.T) {
	_, err := NewMongoDeviceStore(MongoDeviceStoreConfig{Database: "grnoti_test"})
	if err == nil {
		t.Fatal("NewMongoDeviceStore(empty URI) = nil error, want non-nil")
	}
}

func TestNewMongoDeviceStore_EmptyDatabase(t *testing.T) {
	_, err := NewMongoDeviceStore(MongoDeviceStoreConfig{URI: testMongoURI})
	if err == nil {
		t.Fatal("NewMongoDeviceStore(empty Database) = nil error, want non-nil")
	}
}

func TestNewMongoDeviceStore_PingError(t *testing.T) {
	_, err := NewMongoDeviceStore(MongoDeviceStoreConfig{
		URI: "mongodb://127.0.0.1:1/?connectTimeoutMS=500&serverSelectionTimeoutMS=500", Database: "grnoti_test",
	})
	if err == nil {
		t.Fatal("NewMongoDeviceStore(unreachable host) = nil error, want non-nil")
	}
}

func TestNewMongoDeviceStore_DefaultsCollectionName(t *testing.T) {
	store, err := NewMongoDeviceStore(MongoDeviceStoreConfig{
		URI: testMongoURI, Database: fmt.Sprintf("grnoti_test_defaultcoll_%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	defer func() { _ = store.Close() }()
	if got := store.(*mongoDeviceStore).collection.Name(); got != DefaultDeviceCollection {
		t.Fatalf("collection name = %q, want %q (the default)", got, DefaultDeviceCollection)
	}
}

func TestMongoDeviceStore_GetActiveDevicesBatch_SkipsUndecodableDoc(t *testing.T) {
	store := newTestMongoDeviceStore(t)
	s := store.(*mongoDeviceStore)
	ctx := context.Background()

	_ = store.SaveDevice(ctx, Device{FID: testFID("t-good"), UserID: "u1"})
	// is_active with the wrong BSON type: mongoDeviceDoc expects a bool, so
	// this document can't be decoded and must be skipped, not fail the
	// whole batch.
	if _, err := s.collection.InsertOne(ctx, bson.M{
		"fid": "t-bad", "user_id": "u1", "platform": "android", "is_active": "not-a-bool",
		"created_at": time.Now(), "updated_at": time.Now(),
	}); err != nil {
		t.Fatalf("raw insert: %v", err)
	}

	out, err := store.GetActiveDevicesBatch(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("GetActiveDevicesBatch: %v", err)
	}
	if len(out["u1"]) != 1 || out["u1"][0].FID != testFID("t-good") {
		t.Fatalf("GetActiveDevicesBatch()[u1] = %v, want only the well-formed device", out["u1"])
	}
}

func TestMongoDeviceStore_SaveAndGet(t *testing.T) {
	store := newTestMongoDeviceStore(t)
	ctx := context.Background()

	if err := store.SaveDevice(ctx, Device{FID: testFID("t1"), UserID: "u1", Platform: PlatformAndroid}); err != nil {
		t.Fatalf("SaveDevice: %v", err)
	}

	devices, err := store.GetActiveDevices(ctx, "u1")
	if err != nil {
		t.Fatalf("GetActiveDevices: %v", err)
	}
	if len(devices) != 1 || devices[0].FID != testFID("t1") {
		t.Fatalf("GetActiveDevices() = %v, want [t1]", devices)
	}
}

func TestMongoDeviceStore_SaveDevice_UpsertReactivates(t *testing.T) {
	store := newTestMongoDeviceStore(t)
	ctx := context.Background()

	_ = store.SaveDevice(ctx, Device{FID: testFID("t1"), UserID: "u1"})
	_ = store.MarkInvalid(ctx, testFID("t1"))
	if devices, _ := store.GetActiveDevices(ctx, "u1"); len(devices) != 0 {
		t.Fatalf("expected device inactive after MarkInvalid, got %v", devices)
	}

	_ = store.SaveDevice(ctx, Device{FID: testFID("t1"), UserID: "u1"})
	devices, err := store.GetActiveDevices(ctx, "u1")
	if err != nil || len(devices) != 1 {
		t.Fatalf("GetActiveDevices after re-save = (%v, %v), want 1 active device", devices, err)
	}
}

func TestMongoDeviceStore_GetActiveDevicesByAnonymousID(t *testing.T) {
	store := newTestMongoDeviceStore(t)
	ctx := context.Background()
	_ = store.SaveDevice(ctx, Device{FID: testFID("t1"), AnonymousID: "a1"})

	devices, err := store.GetActiveDevicesByAnonymousID(ctx, "a1")
	if err != nil || len(devices) != 1 {
		t.Fatalf("GetActiveDevicesByAnonymousID() = (%v, %v), want 1 device", devices, err)
	}
}

func TestMongoDeviceStore_GetActiveDevicesBatch(t *testing.T) {
	store := newTestMongoDeviceStore(t)
	ctx := context.Background()
	_ = store.SaveDevice(ctx, Device{FID: testFID("t1"), UserID: "u1"})
	_ = store.SaveDevice(ctx, Device{FID: testFID("t2"), UserID: "u2"})

	out, err := store.GetActiveDevicesBatch(ctx, []string{"u1", "u2", "u3"})
	if err != nil || len(out) != 2 {
		t.Fatalf("GetActiveDevicesBatch() = (%v, %v), want 2 users", out, err)
	}
}

func TestMongoDeviceStore_DeleteDevice(t *testing.T) {
	store := newTestMongoDeviceStore(t)
	ctx := context.Background()
	_ = store.SaveDevice(ctx, Device{FID: testFID("t1"), UserID: "u1"})

	if err := store.DeleteDevice(ctx, testFID("t1")); err != nil {
		t.Fatalf("DeleteDevice: %v", err)
	}
	if devices, _ := store.GetActiveDevices(ctx, "u1"); len(devices) != 0 {
		t.Fatalf("device still present after DeleteDevice: %v", devices)
	}
	// Deleting again (nonexistent) is not an error.
	if err := store.DeleteDevice(ctx, testFID("t1")); err != nil {
		t.Fatalf("DeleteDevice (already deleted): %v", err)
	}
}

func TestMongoDeviceStore_Close_Idempotent(t *testing.T) {
	store := newTestMongoDeviceStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil (idempotent)", err)
	}
	if _, err := store.GetActiveDevices(context.Background(), "u1"); err != ErrClosed {
		t.Fatalf("GetActiveDevices after Close error = %v, want ErrClosed", err)
	}
}

// TestMongoDeviceStore_GenericQueryError uses an already-canceled context
// to force a real query-level error from every method — the generic
// (non-ErrClosed) "backend unavailable" wrap branch each method has,
// otherwise unreachable against a healthy database without fault injection.
func TestMongoDeviceStore_GenericQueryError(t *testing.T) {
	store := newTestMongoDeviceStore(t)
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

func TestMongoDeviceStore_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	store := newTestMongoDeviceStore(t)
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
