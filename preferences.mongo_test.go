// File: preferences.mongo_test.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func newTestMongoPreferencesStore(t *testing.T) *mongoPreferencesStore {
	t.Helper()
	collection := fmt.Sprintf("preferences_%d", time.Now().UnixNano())
	store, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{
		URI: testMongoURI, Database: "grnoti_test", CollectionName: collection,
	})
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	cleanupContractCollection(t, "grnoti_test", collection)
	t.Cleanup(func() { _ = store.Close() })
	return store.(*mongoPreferencesStore)
}

func TestNewMongoPreferencesStore_EmptyURI(t *testing.T) {
	if _, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{Database: "grnoti_test"}); err == nil {
		t.Fatal("NewMongoPreferencesStore(empty URI) = nil error, want non-nil")
	}
}

func TestNewMongoPreferencesStore_EmptyDatabase(t *testing.T) {
	if _, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{URI: testMongoURI}); err == nil {
		t.Fatal("NewMongoPreferencesStore(empty Database) = nil error, want non-nil")
	}
}

func TestNewMongoPreferencesStore_PingError(t *testing.T) {
	_, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{
		URI: "mongodb://127.0.0.1:1/?connectTimeoutMS=500&serverSelectionTimeoutMS=500", Database: "grnoti_test",
	})
	if !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("NewMongoPreferencesStore(unreachable host) error = %v, want ErrBackendUnavailable", err)
	}
}

func TestNewMongoPreferencesStore_MalformedURI(t *testing.T) {
	if _, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{URI: "not-a-mongo-uri", Database: "grnoti_test"}); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("NewMongoPreferencesStore(malformed URI) error = %v, want ErrBackendUnavailable", err)
	}
}

// TestNewMongoPreferencesStore_UniqueIndexBuildFails points the store at a
// collection whose existing documents already break one-document-per-user,
// so the unique user_id index can't be built and construction must fail
// rather than run without the guarantee.
func TestNewMongoPreferencesStore_UniqueIndexBuildFails(t *testing.T) {
	ctx := context.Background()
	client, err := mongo.Connect(options.Client().ApplyURI(testMongoURI))
	if err != nil {
		t.Skipf("MongoDB not available, skipping: %v", err)
	}
	defer func() { _ = client.Disconnect(ctx) }()
	collection := fmt.Sprintf("preferences_dupes_%d", time.Now().UnixNano())
	cleanupContractCollection(t, "grnoti_test", collection)
	if _, err := client.Database("grnoti_test").Collection(collection).InsertMany(ctx, []any{
		bson.M{"user_id": "u1"}, bson.M{"user_id": "u1"},
	}); err != nil {
		t.Skipf("MongoDB not available, skipping: %v", err)
	}

	if _, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{
		URI: testMongoURI, Database: "grnoti_test", CollectionName: collection,
	}); err == nil {
		t.Fatal("NewMongoPreferencesStore(duplicate user_ids present) = nil error, want the unique index build to fail")
	}
}

func TestNewMongoPreferencesStore_DefaultsCollectionName(t *testing.T) {
	store, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{URI: testMongoURI, Database: "grnoti_test"})
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	cleanupContractCollection(t, "grnoti_test", DefaultPreferencesCollection)
	defer func() { _ = store.Close() }()
	if got := store.(*mongoPreferencesStore).collection.Name(); got != DefaultPreferencesCollection {
		t.Fatalf("collection name = %q, want %q (the default)", got, DefaultPreferencesCollection)
	}
}

func TestMongoPreferencesStore_RoundTripsEveryField(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	ctx := context.Background()

	// Dotted and $-prefixed event types are legal EventTypes; stored as
	// array elements they never become BSON field names.
	want := &NotificationPreferences{
		UserID: "u1", GlobalEnabled: true, QuietHoursEnabled: true,
		QuietHoursStart: "22:00", QuietHoursEnd: "07:00", Timezone: "Asia/Kolkata", Locale: "hi",
		EventTypeSettings: map[EventType]bool{"order.shipped": false, "$custom": true, EventTypeGenericMarketing: false},
	}
	if err := store.SavePreferences(ctx, want); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}
	got, err := store.GetPreferences(ctx, "u1")
	if err != nil {
		t.Fatalf("GetPreferences: %v", err)
	}
	if got.UserID != want.UserID || got.GlobalEnabled != want.GlobalEnabled ||
		got.QuietHoursEnabled != want.QuietHoursEnabled || got.QuietHoursStart != want.QuietHoursStart ||
		got.QuietHoursEnd != want.QuietHoursEnd || got.Timezone != want.Timezone || got.Locale != want.Locale {
		t.Fatalf("GetPreferences() = %+v, want %+v", got, want)
	}
	if !maps.Equal(got.EventTypeSettings, want.EventTypeSettings) {
		t.Fatalf("EventTypeSettings = %v, want %v", got.EventTypeSettings, want.EventTypeSettings)
	}
	if got.CreatedAt.IsZero() || got.UpdatedAt.IsZero() {
		t.Fatalf("CreatedAt/UpdatedAt = %v/%v, want both set", got.CreatedAt, got.UpdatedAt)
	}

	// Stored sorted by event type, independent of map iteration order.
	var raw mongoPreferencesDoc
	if err := store.collection.FindOne(ctx, bson.M{"user_id": "u1"}).Decode(&raw); err != nil {
		t.Fatalf("raw FindOne: %v", err)
	}
	var order []EventType
	for _, s := range raw.EventTypeSettings {
		order = append(order, s.EventType)
	}
	if fmt.Sprint(order) != fmt.Sprint([]EventType{"$custom", EventTypeGenericMarketing, "order.shipped"}) {
		t.Fatalf("stored event_type_settings order = %v, want sorted", order)
	}
}

func TestMongoPreferencesStore_SaveIsUpsertKeepsCreatedAt(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	ctx := context.Background()

	if err := store.SavePreferences(ctx, &NotificationPreferences{
		UserID: "u1", GlobalEnabled: true, Locale: "en",
		EventTypeSettings: map[EventType]bool{EventTypeGenericMarketing: false},
	}); err != nil {
		t.Fatalf("first SavePreferences: %v", err)
	}
	first, err := store.GetPreferences(ctx, "u1")
	if err != nil {
		t.Fatalf("first GetPreferences: %v", err)
	}

	time.Sleep(10 * time.Millisecond)
	if err := store.SavePreferences(ctx, &NotificationPreferences{
		UserID: "u1", GlobalEnabled: false, Locale: "fr",
		EventTypeSettings: map[EventType]bool{EventTypeSystemAlert: true},
	}); err != nil {
		t.Fatalf("second SavePreferences: %v", err)
	}
	second, err := store.GetPreferences(ctx, "u1")
	if err != nil {
		t.Fatalf("second GetPreferences: %v", err)
	}

	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Errorf("CreatedAt changed on upsert: %v -> %v", first.CreatedAt, second.CreatedAt)
	}
	if !second.UpdatedAt.After(first.UpdatedAt) {
		t.Errorf("UpdatedAt did not advance on upsert: %v -> %v", first.UpdatedAt, second.UpdatedAt)
	}
	if second.GlobalEnabled || second.Locale != "fr" {
		t.Errorf("fields not overwritten: GlobalEnabled=%v Locale=%q, want false/fr", second.GlobalEnabled, second.Locale)
	}
	// Replaced wholesale, as the Postgres upsert does — not merged.
	if !maps.Equal(second.EventTypeSettings, map[EventType]bool{EventTypeSystemAlert: true}) {
		t.Errorf("EventTypeSettings = %v, want only the second save's", second.EventTypeSettings)
	}
	if n, err := store.collection.CountDocuments(ctx, bson.M{"user_id": "u1"}); err != nil || n != 1 {
		t.Errorf("documents for u1 = (%d, %v), want (1, nil)", n, err)
	}
}

func TestMongoPreferencesStore_EmptySettingsDecodeAsEmptyMap(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	ctx := context.Background()

	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "u1", GlobalEnabled: true}); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}
	got, err := store.GetPreferences(ctx, "u1")
	if err != nil {
		t.Fatalf("GetPreferences: %v", err)
	}
	if got.EventTypeSettings == nil || len(got.EventTypeSettings) != 0 {
		t.Fatalf("EventTypeSettings = %#v, want a non-nil empty map", got.EventTypeSettings)
	}

	var raw bson.M
	if err := store.collection.FindOne(ctx, bson.M{"user_id": "u1"}).Decode(&raw); err != nil {
		t.Fatalf("raw FindOne: %v", err)
	}
	if arr, ok := raw["event_type_settings"].(bson.A); !ok || len(arr) != 0 {
		t.Fatalf("stored event_type_settings = %#v, want an empty array, not null", raw["event_type_settings"])
	}
}

func TestMongoPreferencesStore_UniqueIndexOnUserID(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	ctx := context.Background()

	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "u1"}); err != nil {
		t.Fatalf("SavePreferences: %v", err)
	}
	_, err := store.collection.InsertOne(ctx, bson.M{"user_id": "u1"})
	if !mongo.IsDuplicateKeyError(err) {
		t.Fatalf("raw duplicate insert error = %v, want a duplicate-key error from the unique user_id index", err)
	}
}

func TestMongoPreferencesStore_SaveRequiresUserID(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	if err := store.SavePreferences(context.Background(), &NotificationPreferences{}); err != ErrPreferencesUserIDRequired {
		t.Fatalf("SavePreferences(empty UserID) error = %v, want ErrPreferencesUserIDRequired", err)
	}
}

// TestMongoPreferencesStore_GenericQueryError uses an already-canceled
// context to force a real query-level error from every method, as
// TestMongoDeviceStore_GenericQueryError does.
func TestMongoPreferencesStore_GenericQueryError(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := store.GetPreferences(ctx, "u1"); !errors.Is(err, ErrBackendUnavailable) {
		t.Errorf("GetPreferences(canceled ctx) error = %v, want ErrBackendUnavailable", err)
	}
	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "u1"}); !errors.Is(err, ErrBackendUnavailable) {
		t.Errorf("SavePreferences(canceled ctx) error = %v, want ErrBackendUnavailable", err)
	}
	if enabled, err := store.IsEventTypeEnabled(ctx, "u1", EventTypeSystemAlert); err == nil || enabled {
		t.Errorf("IsEventTypeEnabled(canceled ctx) = (%v, %v), want (false, error)", enabled, err)
	}
}

func TestMongoPreferencesStore_Close_Idempotent(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil (idempotent)", err)
	}
}

func TestMongoPreferencesStore_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	store := newTestMongoPreferencesStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if _, err := store.GetPreferences(ctx, "u1"); err != ErrClosed {
		t.Errorf("GetPreferences after Close = %v, want ErrClosed", err)
	}
	if err := store.SavePreferences(ctx, &NotificationPreferences{UserID: "u1"}); err != ErrClosed {
		t.Errorf("SavePreferences after Close = %v, want ErrClosed", err)
	}
	if _, err := store.IsEventTypeEnabled(ctx, "u1", EventTypeSystemAlert); err != ErrClosed {
		t.Errorf("IsEventTypeEnabled after Close = %v, want ErrClosed", err)
	}
}
