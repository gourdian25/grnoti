// File: tokenstore.mongo.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DefaultTokenCollection is the collection name used when
// MongoTokenStoreConfig.CollectionName is empty.
const DefaultTokenCollection = "grnoti_tokens"

type mongoTokenDoc struct {
	Token       string    `bson:"token"`
	Platform    Platform  `bson:"platform"`
	UserID      string    `bson:"user_id,omitempty"`
	AnonymousID string    `bson:"anonymous_id,omitempty"`
	DeviceID    string    `bson:"device_id,omitempty"`
	AppVersion  string    `bson:"app_version,omitempty"`
	IsActive    bool      `bson:"is_active"`
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at"`
}

func (d mongoTokenDoc) toDeviceToken() DeviceToken {
	// mongoTokenDoc's fields are identical to DeviceToken's in name, type,
	// and order (only the struct tags differ, which Go ignores for
	// conversion identity), so a direct conversion is equivalent to and
	// safer than a field-by-field literal that could silently drift.
	return DeviceToken(d)
}

// MongoTokenStoreConfig configures a TokenStore constructed by
// NewMongoTokenStore. Following grcache's pattern (not gourdiantoken's) —
// see docs/plan/grnoti-plan.md §1.5 — the store owns and connects its own
// *mongo.Client from URI, rather than taking an already-connected
// *mongo.Database from the caller.
type MongoTokenStoreConfig struct {
	// URI is a standard MongoDB connection string. Required.
	URI string
	// Database is the database name. Required.
	Database string
	// CollectionName defaults to DefaultTokenCollection if empty.
	CollectionName string
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

type mongoTokenStore struct {
	client     *mongo.Client
	collection *mongo.Collection
	logger     Logger

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ TokenStore = (*mongoTokenStore)(nil)

// NewMongoTokenStore connects to MongoDB per cfg, ensures indexes, and
// validates connectivity via Ping before returning.
func NewMongoTokenStore(cfg MongoTokenStoreConfig) (TokenStore, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoTokenStoreConfig.URI is required")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoTokenStoreConfig.Database is required")
	}
	collName := cfg.CollectionName
	if collName == "" {
		collName = DefaultTokenCollection
	}
	logger := OrNop(cfg.Logger)

	connectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(connectCtx, options.Client().ApplyURI(cfg.URI))
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: connect: %w", errors.Join(err, ErrBackendUnavailable))
	}
	if err := client.Ping(connectCtx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("grnoti/mongo: ping: %w", errors.Join(err, ErrBackendUnavailable))
	}

	collection := client.Database(cfg.Database).Collection(collName)
	if _, err := collection.Indexes().CreateMany(connectCtx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "token", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "is_active", Value: 1}}},
		{Keys: bson.D{{Key: "anonymous_id", Value: 1}, {Key: "is_active", Value: 1}}},
	}); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("grnoti/mongo: ensure indexes: %w", err)
	}

	logger.Info("grnoti/mongo: token store connected", "database", cfg.Database, "collection", collName)
	return &mongoTokenStore{client: client, collection: collection, logger: logger}, nil
}

func (s *mongoTokenStore) GetActiveTokens(ctx context.Context, userID string) ([]DeviceToken, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	cursor, err := s.collection.Find(ctx, bson.M{"user_id": userID, "is_active": true})
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active tokens for %s: %w", userID, errors.Join(err, ErrBackendUnavailable))
	}
	defer func() { _ = cursor.Close(ctx) }()

	var docs []mongoTokenDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("grnoti/mongo: decode tokens for %s: %w", userID, err)
	}
	out := make([]DeviceToken, len(docs))
	for i, d := range docs {
		out[i] = d.toDeviceToken()
	}
	return out, nil
}

func (s *mongoTokenStore) GetActiveTokensBatch(ctx context.Context, userIDs []string) (map[string][]DeviceToken, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	cursor, err := s.collection.Find(ctx, bson.M{"user_id": bson.M{"$in": userIDs}, "is_active": true})
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active tokens batch: %w", errors.Join(err, ErrBackendUnavailable))
	}
	defer func() { _ = cursor.Close(ctx) }()

	out := make(map[string][]DeviceToken)
	for cursor.Next(ctx) {
		var doc mongoTokenDoc
		if err := cursor.Decode(&doc); err != nil {
			s.logger.Warn("grnoti/mongo: skipping undecodable token document", "error", err)
			continue
		}
		out[doc.UserID] = append(out[doc.UserID], doc.toDeviceToken())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active tokens batch: %w", err)
	}
	return out, nil
}

func (s *mongoTokenStore) GetActiveTokensByAnonymousID(ctx context.Context, anonymousID string) ([]DeviceToken, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	cursor, err := s.collection.Find(ctx, bson.M{"anonymous_id": anonymousID, "is_active": true})
	if err != nil {
		return nil, fmt.Errorf("grnoti/mongo: get active tokens for anonymous %s: %w", anonymousID, errors.Join(err, ErrBackendUnavailable))
	}
	defer func() { _ = cursor.Close(ctx) }()

	var docs []mongoTokenDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("grnoti/mongo: decode tokens for anonymous %s: %w", anonymousID, err)
	}
	out := make([]DeviceToken, len(docs))
	for i, d := range docs {
		out[i] = d.toDeviceToken()
	}
	return out, nil
}

func (s *mongoTokenStore) MarkInvalid(ctx context.Context, token string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	res, err := s.collection.UpdateOne(ctx,
		bson.M{"token": token},
		bson.M{"$set": bson.M{"is_active": false, "updated_at": time.Now().UTC()}},
	)
	if err != nil {
		return fmt.Errorf("grnoti/mongo: mark invalid %s: %w", token, errors.Join(err, ErrBackendUnavailable))
	}
	if res.MatchedCount == 0 {
		s.logger.Debug("grnoti/mongo: MarkInvalid: token not found", "token", token)
	}
	return nil
}

func (s *mongoTokenStore) SaveToken(ctx context.Context, token DeviceToken) error {
	if s.closed.Load() {
		return ErrClosed
	}
	now := time.Now().UTC()
	_, err := s.collection.UpdateOne(ctx,
		bson.M{"token": token.Token},
		bson.M{
			"$set": bson.M{
				"platform": token.Platform, "user_id": token.UserID, "anonymous_id": token.AnonymousID,
				"device_id": token.DeviceID, "app_version": token.AppVersion, "is_active": true, "updated_at": now,
			},
			"$setOnInsert": bson.M{"created_at": now},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("grnoti/mongo: save token: %w", errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (s *mongoTokenStore) DeleteToken(ctx context.Context, token string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	res, err := s.collection.DeleteOne(ctx, bson.M{"token": token})
	if err != nil {
		return fmt.Errorf("grnoti/mongo: delete token %s: %w", token, errors.Join(err, ErrBackendUnavailable))
	}
	if res.DeletedCount == 0 {
		s.logger.Debug("grnoti/mongo: DeleteToken: token not found", "token", token)
	}
	return nil
}

func (s *mongoTokenStore) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		err = s.client.Disconnect(context.Background())
		s.logger.Info("grnoti/mongo: token store closed")
	})
	return err
}
