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
