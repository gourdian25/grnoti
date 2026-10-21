// File: preferences.mongo.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DefaultPreferencesCollection is the collection name used when
// MongoPreferencesStoreConfig.CollectionName is empty.
const DefaultPreferencesCollection = "grnoti_preferences"

// mongoPreferencesDoc is one user's preferences document. EventTypeSettings
// is stored as an array of {event_type, enabled} pairs, not a sub-document
// keyed by event type: EventType is any non-empty string, so a caller's
// "order.shipped" or "$custom" would be a dotted or $-prefixed BSON field
// name, which Mongo treats as a path or operator in updates. Sorted by
// event type on write so a document's bytes don't depend on map iteration
// order.
type mongoPreferencesDoc struct {
	UserID            string                  `bson:"user_id"`
	GlobalEnabled     bool                    `bson:"global_enabled"`
	QuietHoursEnabled bool                    `bson:"quiet_hours_enabled"`
	QuietHoursStart   string                  `bson:"quiet_hours_start"`
	QuietHoursEnd     string                  `bson:"quiet_hours_end"`
	Timezone          string                  `bson:"timezone"`
	Locale            string                  `bson:"locale"`
	EventTypeSettings []mongoEventTypeSetting `bson:"event_type_settings"`
	CreatedAt         time.Time               `bson:"created_at"`
	UpdatedAt         time.Time               `bson:"updated_at"`
}

type mongoEventTypeSetting struct {
	EventType EventType `bson:"event_type"`
	Enabled   bool      `bson:"enabled"`
}

// toPreferences converts d back to the domain type. EventTypeSettings is
// never nil, matching preferencesRowToDomain.
func (d mongoPreferencesDoc) toPreferences() *NotificationPreferences {
	settings := make(map[EventType]bool, len(d.EventTypeSettings))
	for _, s := range d.EventTypeSettings {
		settings[s.EventType] = s.Enabled
	}
	return &NotificationPreferences{
		UserID: d.UserID, GlobalEnabled: d.GlobalEnabled, QuietHoursEnabled: d.QuietHoursEnabled,
		QuietHoursStart: d.QuietHoursStart, QuietHoursEnd: d.QuietHoursEnd, Timezone: d.Timezone, Locale: d.Locale,
		EventTypeSettings: settings, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
	}
}

// settingsToMongo turns a settings map into the stored array, sorted by
// event type. Never nil, so an unconfigured user stores [] rather than null.
func settingsToMongo(settings map[EventType]bool) []mongoEventTypeSetting {
	out := make([]mongoEventTypeSetting, 0, len(settings))
	for eventType, enabled := range settings {
		out = append(out, mongoEventTypeSetting{EventType: eventType, Enabled: enabled})
	}
	slices.SortFunc(out, func(a, b mongoEventTypeSetting) int { return strings.Compare(string(a.EventType), string(b.EventType)) })
	return out
}

// MongoPreferencesStoreConfig configures a PreferencesStore constructed by
// NewMongoPreferencesStore. Like MongoDeviceStoreConfig, the store owns and
// connects its own *mongo.Client from URI, rather than taking an
// already-connected *mongo.Database from the caller.
type MongoPreferencesStoreConfig struct {
	// URI is a standard MongoDB connection string. Required.
	URI string
	// Database is the database name. Required.
	Database string
	// CollectionName defaults to DefaultPreferencesCollection if empty.
	CollectionName string
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

type mongoPreferencesStore struct {
	client     *mongo.Client
	collection *mongo.Collection
	logger     Logger

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ PreferencesStore = (*mongoPreferencesStore)(nil)

// NewMongoPreferencesStore connects to MongoDB per cfg, ensures the unique
// user_id index (one document per user), and validates connectivity via
// Ping before returning. It behaves exactly as NewPostgresPreferencesStore
// does: an unknown user is ErrPreferencesNotFound, SavePreferences is an
// upsert that keeps CreatedAt, and an unconfigured user is opted in.
func NewMongoPreferencesStore(cfg MongoPreferencesStoreConfig) (PreferencesStore, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoPreferencesStoreConfig.URI is required")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoPreferencesStoreConfig.Database is required")
	}
	collName := cfg.CollectionName
	if collName == "" {
		collName = DefaultPreferencesCollection
	}
	logger := OrNop(cfg.Logger)

	// v2's mongo.Connect doesn't block on the network; Ping is the real
	// connectivity check (see NewMongoDeviceStore).
	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: connect: %w", errors.Join(err, ErrBackendUnavailable))
	}
	if err := client.Ping(connectCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("grnoti/mongo: ping: %w", errors.Join(err, ErrBackendUnavailable))
	}

	collection := client.Database(cfg.Database).Collection(collName)
	if _, err := collection.Indexes().CreateOne(connectCtx, mongo.IndexModel{
		Keys: bson.D{{Key: "user_id", Value: 1}}, Options: options.Index().SetUnique(true),
	}); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("grnoti/mongo: ensure indexes: %w", err)
	}

	logger.Info("grnoti/mongo: preferences store connected", "database", cfg.Database, "collection", collName)
	return &mongoPreferencesStore{client: client, collection: collection, logger: logger}, nil
}

func (s *mongoPreferencesStore) GetPreferences(ctx context.Context, userID string) (*NotificationPreferences, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	var doc mongoPreferencesDoc
	err := s.collection.FindOne(ctx, bson.M{"user_id": userID}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, ErrPreferencesNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get preferences for %s: %w", userID, errors.Join(err, ErrBackendUnavailable))
	}
	return doc.toPreferences(), nil
}

// SavePreferences upserts prefs as the Postgres store's
// ON CONFLICT DO UPDATE does: every field is overwritten (EventTypeSettings
// replaced, not merged), UpdatedAt refreshed, CreatedAt set only on insert.
// Two first-time saves for one user can't create two documents: MongoDB
// retries an upsert that loses a duplicate-key race on an equality match
// against a unique index.
func (s *mongoPreferencesStore) SavePreferences(ctx context.Context, prefs *NotificationPreferences) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if prefs.UserID == "" {
		return ErrPreferencesUserIDRequired
	}
	now := time.Now().UTC()
	_, err := s.collection.UpdateOne(ctx,
		bson.M{"user_id": prefs.UserID},
		bson.M{
			"$set": bson.M{
				"global_enabled": prefs.GlobalEnabled, "quiet_hours_enabled": prefs.QuietHoursEnabled,
				"quiet_hours_start": prefs.QuietHoursStart, "quiet_hours_end": prefs.QuietHoursEnd,
				"timezone": prefs.Timezone, "locale": prefs.Locale,
				"event_type_settings": settingsToMongo(prefs.EventTypeSettings), "updated_at": now,
			},
			"$setOnInsert": bson.M{"created_at": now},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("grnoti/mongo: save preferences for %s: %w", prefs.UserID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (s *mongoPreferencesStore) IsEventTypeEnabled(ctx context.Context, userID string, eventType EventType) (bool, error) {
	prefs, err := s.GetPreferences(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrPreferencesNotFound) {
			return true, nil
		}
		return false, err
	}
	return prefs.IsEventTypeEnabled(eventType), nil
}

func (s *mongoPreferencesStore) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		err = s.client.Disconnect(context.Background())
		s.logger.Info("grnoti/mongo: preferences store closed")
	})
	return err
}
