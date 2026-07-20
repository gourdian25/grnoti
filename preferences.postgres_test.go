// File: preferences.postgres_test.go

package grnoti

import (
	"context"
	"testing"
)

func newTestPostgresPreferencesStore(t *testing.T) PreferencesStore {
	t.Helper()
	store, err := NewPostgresPreferencesStore(PostgresConfig{DSN: testPostgresDSN})
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	t.Cleanup(func() {
		if s, ok := store.(*postgresPreferencesStore); ok {
			s.pool.Exec(context.Background(), "DELETE FROM grnoti_preferences")
		}
		_ = store.Close()
	})
	return store
}

func TestNewPostgresPreferencesStore_ConnectError(t *testing.T) {
	if _, err := NewPostgresPreferencesStore(PostgresConfig{}); err == nil {
		t.Fatal("NewPostgresPreferencesStore(empty DSN) = nil error, want non-nil")
	}
}

func TestPostgresPreferencesStore_NotFoundThenSave(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	ctx := context.Background()

	if _, err := store.GetPreferences(ctx, "pgu1"); err != ErrPreferencesNotFound {
		t.Fatalf("GetPreferences (unset) error = %v, want ErrPreferencesNotFound", err)
	}

	if err := store.SavePreferences(ctx, &NotificationPreferences{
		UserID: "pgu1", GlobalEnabled: true, Locale: "en",
		EventTypeSettings: map[EventType]bool{EventTypeGenericMarketing: false},
	}); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}

	got, err := store.GetPreferences(ctx, "pgu1")
	if err != nil {
		t.Fatalf("GetPreferences: %v", err)
	}
	if !got.GlobalEnabled || got.Locale != "en" || got.EventTypeSettings[EventTypeGenericMarketing] != false {
		t.Fatalf("GetPreferences() = %+v, want matching what was saved", got)
	}
}

func TestPostgresPreferencesStore_SaveUpsertPreservesCreatedAt(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	ctx := context.Background()

	_ = store.SavePreferences(ctx, &NotificationPreferences{UserID: "pgu2", GlobalEnabled: true})
	first, _ := store.GetPreferences(ctx, "pgu2")

	_ = store.SavePreferences(ctx, &NotificationPreferences{UserID: "pgu2", GlobalEnabled: false})
	second, _ := store.GetPreferences(ctx, "pgu2")

	if second.GlobalEnabled {
		t.Fatal("SavePreferences (update) did not persist the new GlobalEnabled value")
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("CreatedAt changed across an update: first=%v second=%v", first.CreatedAt, second.CreatedAt)
	}
}

func TestPostgresPreferencesStore_IsEventTypeEnabled(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	ctx := context.Background()

	enabled, err := store.IsEventTypeEnabled(ctx, "never-seen-pg", EventTypeSystemAlert)
	if err != nil || !enabled {
		t.Fatalf("IsEventTypeEnabled(unconfigured) = (%v, %v), want (true, nil)", enabled, err)
	}

	_ = store.SavePreferences(ctx, &NotificationPreferences{
		UserID: "pgu3", GlobalEnabled: true,
		EventTypeSettings: map[EventType]bool{EventTypeGenericMarketing: false},
	})
	enabled, _ = store.IsEventTypeEnabled(ctx, "pgu3", EventTypeGenericMarketing)
	if enabled {
		t.Fatal("IsEventTypeEnabled(opted-out) = true, want false")
	}
}

func TestPostgresPreferencesStore_Close_Idempotent(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
}

// TestPostgresPreferencesStore_GenericQueryError uses an already-canceled
// context to force a real query-level error — see the analogous
// tokenstore.postgres_test.go comment for why this reaches a branch fault
// injection would otherwise require.
func TestPostgresPreferencesStore_GenericQueryError(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.GetPreferences(ctx, "pgu1"); err == nil {
		t.Error("GetPreferences(canceled ctx) = nil error, want non-nil")
	}
	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "pgu1"}); err == nil {
		t.Error("SavePreferences(canceled ctx) = nil error, want non-nil")
	}
}

func TestPostgresPreferencesStore_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if _, err := store.GetPreferences(ctx, "pgu1"); err != ErrClosed {
		t.Errorf("GetPreferences after Close = %v, want ErrClosed", err)
	}
	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "pgu1"}); err != ErrClosed {
		t.Errorf("SavePreferences after Close = %v, want ErrClosed", err)
	}
	if _, err := store.IsEventTypeEnabled(ctx, "pgu1", EventTypeSystemAlert); err != ErrClosed {
		t.Errorf("IsEventTypeEnabled after Close = %v, want ErrClosed (delegates to GetPreferences)", err)
	}
}

// TestPreferencesRowToDomain_MalformedEventTypeSettings inserts a row whose
// event_type_settings is valid JSON (JSONB itself guarantees that) but not
// the expected shape (an object, not a map[EventType]bool-compatible one)
// — exercising preferencesRowToDomain's json.Unmarshal error branch, which
// a plain malformed-syntax insert could never reach since Postgres rejects
// non-JSON text at the JSONB column level before it's even stored.
func TestPreferencesRowToDomain_MalformedEventTypeSettings(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	s, ok := store.(*postgresPreferencesStore)
	if !ok {
		t.Fatal("store is not *postgresPreferencesStore")
	}
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `INSERT INTO grnoti_preferences
		(user_id, global_enabled, quiet_hours_enabled, event_type_settings, created_at, updated_at)
		VALUES ($1, true, false, '"not an object"', now(), now())`, "pgu-malformed")
	if err != nil {
		t.Fatalf("raw insert: %v", err)
	}

	if _, err := store.GetPreferences(ctx, "pgu-malformed"); err == nil {
		t.Fatal("GetPreferences(malformed event_type_settings) = nil error, want non-nil")
	}
}

// TestPostgresPreferencesStore_SavePreferences_EmptyUserID matches the
// analogous validation every other PreferencesStore backend enforces —
// closing a real coverage gap where this backend's own empty-UserID
// rejection path went untested.
func TestPostgresPreferencesStore_SavePreferences_EmptyUserID(t *testing.T) {
	store := newTestPostgresPreferencesStore(t)
	err := store.SavePreferences(context.Background(), &NotificationPreferences{GlobalEnabled: true})
	if err != ErrNoTargetSpecified {
		t.Fatalf("SavePreferences(empty UserID) error = %v, want ErrNoTargetSpecified", err)
	}
}
