// File: devicestore.mongo.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// DefaultDeviceCollection is the collection name used when
// MongoDeviceStoreConfig.CollectionName is empty.
const DefaultDeviceCollection = "grnoti_devices"

type mongoDeviceDoc struct {
	FID         string    `bson:"fid"`
	Platform    Platform  `bson:"platform"`
	UserID      string    `bson:"user_id,omitempty"`
	AnonymousID string    `bson:"anonymous_id,omitempty"`
	DeviceID    string    `bson:"device_id,omitempty"`
	AppVersion  string    `bson:"app_version,omitempty"`
	IsActive    bool      `bson:"is_active"`
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at"`
}

func (d mongoDeviceDoc) toDevice() Device {
	// mongoDeviceDoc's fields are identical to Device's in name, type,
	// and order (only the struct tags differ, which Go ignores for
	// conversion identity), so a direct conversion is equivalent to and
	// safer than a field-by-field literal that could silently drift.
	return Device(d)
}

// MongoDeviceStoreConfig configures a DeviceStore constructed by
// NewMongoDeviceStore. Following grcache's pattern (not gourdiantoken's) —
// see docs/plan/grnoti-plan.md §1.5 — the store owns and connects its own
// *mongo.Client from URI, rather than taking an already-connected
// *mongo.Database from the caller.
type MongoDeviceStoreConfig struct {
	// URI is a standard MongoDB connection string. Required.
	URI string
	// Database is the database name. Required.
	Database string
	// CollectionName defaults to DefaultDeviceCollection if empty.
	CollectionName string
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

type mongoDeviceStore struct {
	client     *mongo.Client
	collection *mongo.Collection
	logger     Logger

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ DeviceStore = (*mongoDeviceStore)(nil)

// NewMongoDeviceStore connects to MongoDB per cfg, ensures indexes, and
// validates connectivity via Ping before returning.
func NewMongoDeviceStore(cfg MongoDeviceStoreConfig) (DeviceStore, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoDeviceStoreConfig.URI is required")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoDeviceStoreConfig.Database is required")
	}
	collName := cfg.CollectionName
	if collName == "" {
		collName = DefaultDeviceCollection
	}
	logger := OrNop(cfg.Logger)

	// v2's mongo.Connect no longer takes a context or blocks on the network;
	// Ping is the real connectivity check, so the timeout that used to bound
	// Connect now bounds that instead (matching grsentry's pkg/grmongo).
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
	if _, err := collection.Indexes().CreateMany(connectCtx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "fid", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "is_active", Value: 1}}},
		{Keys: bson.D{{Key: "anonymous_id", Value: 1}, {Key: "is_active", Value: 1}}},
	}); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("grnoti/mongo: ensure indexes: %w", err)
	}

	logger.Info("grnoti/mongo: device store connected", "database", cfg.Database, "collection", collName)
	return &mongoDeviceStore{client: client, collection: collection, logger: logger}, nil
}

func (s *mongoDeviceStore) GetActiveDevices(ctx context.Context, userID string) ([]Device, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	cursor, err := s.collection.Find(ctx, bson.M{"user_id": userID, "is_active": true})
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active devices for %s: %w", userID, errors.Join(err, ErrBackendUnavailable))
	}
	defer func() { _ = cursor.Close(ctx) }()

	var docs []mongoDeviceDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("grnoti/mongo: decode devices for %s: %w", userID, err)
	}
	out := make([]Device, len(docs))
	for i, d := range docs {
		out[i] = d.toDevice()
	}
	return out, nil
}

func (s *mongoDeviceStore) GetActiveDevicesBatch(ctx context.Context, userIDs []string) (map[string][]Device, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	cursor, err := s.collection.Find(ctx, bson.M{"user_id": bson.M{"$in": userIDs}, "is_active": true})
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active devices batch: %w", errors.Join(err, ErrBackendUnavailable))
	}
	defer func() { _ = cursor.Close(ctx) }()

	out := make(map[string][]Device)
	for cursor.Next(ctx) {
		var doc mongoDeviceDoc
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Warn("grnoti/mongo: skipping undecodable device document", "error", err)
			continue
		}
		out[doc.UserID] = append(out[doc.UserID], doc.toDevice())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active devices batch: %w", err)
	}
	return out, nil
}

func (s *mongoDeviceStore) GetActiveDevicesByAnonymousID(ctx context.Context, anonymousID string) ([]Device, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	cursor, err := s.collection.Find(ctx, bson.M{"anonymous_id": anonymousID, "is_active": true})
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active devices for anonymous %s: %w", anonymousID, errors.Join(err, ErrBackendUnavailable))
	}
	defer func() { _ = cursor.Close(ctx) }()

	var docs []mongoDeviceDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("grnoti/mongo: decode devices for anonymous %s: %w", anonymousID, err)
	}
	out := make([]Device, len(docs))
	for i, d := range docs {
		out[i] = d.toDevice()
	}
	return out, nil
}

func (s *mongoDeviceStore) MarkInvalid(ctx context.Context, fid string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	res, err := s.collection.UpdateOne(ctx,
		bson.M{"fid": fid},
		bson.M{"$set": bson.M{"is_active": false, "updated_at": time.Now().UTC()}},
	)
	if err != nil {
		return fmt.Errorf("grnoti/mongo: mark invalid %s: %w", fid, errors.Join(err, ErrBackendUnavailable))
	}
	if res.MatchedCount == 0 {
		s.logger.Debug("grnoti/mongo: MarkInvalid: fid not found", "fid", maskFID(fid))
	}
	return nil
}

func (s *mongoDeviceStore) SaveDevice(ctx context.Context, device Device) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := device.Validate(); err != nil {
		return err
	}
	now := time.Now().UTC()
	_, err := s.collection.UpdateOne(ctx,
		bson.M{"fid": device.FID},
		bson.M{
			"$set": bson.M{
				"platform": device.Platform, "user_id": device.UserID, "anonymous_id": device.AnonymousID,
				"device_id": device.DeviceID, "app_version": device.AppVersion, "is_active": true, "updated_at": now,
			},
			"$setOnInsert": bson.M{"created_at": now},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("grnoti/mongo: save device: %w", errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (s *mongoDeviceStore) DeleteDevice(ctx context.Context, fid string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	res, err := s.collection.DeleteOne(ctx, bson.M{"fid": fid})
	if err != nil {
		return fmt.Errorf("grnoti/mongo: delete device %s: %w", fid, errors.Join(err, ErrBackendUnavailable))
	}
	if res.DeletedCount == 0 {
		s.logger.Debug("grnoti/mongo: DeleteDevice: fid not found", "fid", maskFID(fid))
	}
	return nil
}

func (s *mongoDeviceStore) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		err = s.client.Disconnect(context.Background())
		s.logger.Info("grnoti/mongo: device store closed")
	})
	return err
}
