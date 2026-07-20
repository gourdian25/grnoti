// File: dlq.mongo.go

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

// DefaultDLQCollection is the collection name used when
// MongoDLQHandlerConfig.CollectionName is empty.
const DefaultDLQCollection = "grnoti_dlq"

type mongoDLQDoc struct {
	EventID        string            `bson:"event_id"`
	Event          Event             `bson:"event"`
	FailureReason  string            `bson:"failure_reason"`
	RetryCount     int               `bson:"retry_count"`
	MaxRetries     int               `bson:"max_retries"`
	FirstFailureAt time.Time         `bson:"first_failure_at"`
	LastAttemptAt  time.Time         `bson:"last_attempt_at"`
	NextRetryAt    time.Time         `bson:"next_retry_at"`
	Status         DLQStatus         `bson:"status"`
	AttemptHistory []DLQRetryAttempt `bson:"attempt_history"`
	CreatedAt      time.Time         `bson:"created_at"`
	UpdatedAt      time.Time         `bson:"updated_at"`
}

func (d mongoDLQDoc) toDLQEvent() *DLQEvent {
	return &DLQEvent{
		EventID: d.EventID, Event: d.Event, FailureReason: d.FailureReason,
		RetryCount: d.RetryCount, MaxRetries: d.MaxRetries,
		FirstFailureAt: d.FirstFailureAt, LastAttemptAt: d.LastAttemptAt, NextRetryAt: d.NextRetryAt,
		Status: d.Status, AttemptHistory: d.AttemptHistory,
		CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
	}
}

// MongoDLQHandlerConfig configures a DLQHandler constructed by
// NewMongoDLQHandler.
type MongoDLQHandlerConfig struct {
	URI            string
	Database       string
	CollectionName string        // defaults to DefaultDLQCollection
	MaxRetries     int           // defaults to 3
	RetryDelay     time.Duration // passed through as-is; 0 means immediately retry-eligible
	MaxRetryDelay  time.Duration // passed through as-is to FullJitterBackoff (0 there means its own internal default ceiling)
	Logger         Logger
}

type mongoDLQHandler struct {
	client        *mongo.Client
	collection    *mongo.Collection
	maxRetries    int
	retryDelay    time.Duration
	maxRetryDelay time.Duration
	logger        Logger

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ DLQHandler = (*mongoDLQHandler)(nil)

// NewMongoDLQHandler connects to MongoDB per cfg, ensures indexes
// (including a 7-day TTL index on created_at as a durable-retention
// backstop independent of PurgeExpiredEvents), and validates connectivity
// before returning.
//
// Claim semantics (see docs/plan/grnoti-plan.md §1.3, §5): unlike the
// reference implementation's MongoDLQHandler.MarkRetried, which read
// RetryCount then wrote it back with no guard at all (a confirmed
// lost-update race under concurrent retries), every write here is scoped
// by an atomic MongoDB operation — ClaimRetryableEvents uses
// FindOneAndUpdate per document (atomic per-document claim, no transaction
// needed), and MarkRetried's retry_count increment is a $inc scoped to
// {event_id, status: "retrying"} rather than a Go-side read-then-set.
func NewMongoDLQHandler(cfg MongoDLQHandlerConfig) (DLQHandler, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoDLQHandlerConfig.URI is required")
	}
	if cfg.Database == "" {
		return nil, fmt.Errorf("grnoti/mongo: MongoDLQHandlerConfig.Database is required")
	}
	collName := cfg.CollectionName
	if collName == "" {
		collName = DefaultDLQCollection
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	// RetryDelay/MaxRetryDelay are passed through unchanged, including 0 —
	// unlike MaxRetries, 0 is a valid, deliberate choice here (immediate
	// retry-eligibility, useful for tests), not silently replaced with a
	// default. See NewMemoryDLQHandler's identical convention.
	retryDelay := cfg.RetryDelay
	maxRetryDelay := cfg.MaxRetryDelay
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
		{Keys: bson.D{{Key: "event_id", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "status", Value: 1}, {Key: "next_retry_at", Value: 1}}},
		{Keys: bson.D{{Key: "created_at", Value: 1}}, Options: options.Index().SetExpireAfterSeconds(7 * 24 * 60 * 60)},
	}); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("grnoti/mongo: ensure indexes: %w", err)
	}

	logger.Infof("grnoti/mongo: dlq handler connected (database=%s collection=%s)", cfg.Database, collName)
	return &mongoDLQHandler{
		client: client, collection: collection,
		maxRetries: maxRetries, retryDelay: retryDelay, maxRetryDelay: maxRetryDelay,
		logger: logger,
	}, nil
}

func (h *mongoDLQHandler) PublishToDLQ(ctx context.Context, event Event, failureReason string) error {
	if h.closed.Load() {
		return ErrClosed
	}
	now := time.Now().UTC()
	attempt := DLQRetryAttempt{AttemptedAt: now, Success: false, ErrorMessage: failureReason}

	// A single atomic upsert handles both "new failure" (insert) and
	// "another failure for an event already pending/retrying" (update) in
	// one write path — unlike the reference implementation, which had two
	// separate, uncoordinated writers for these two cases (see
	// docs/plan/grnoti-plan.md §3.5).
	_, err := h.collection.UpdateOne(ctx,
		bson.M{"event_id": event.EventID},
		bson.M{
			"$push": bson.M{"attempt_history": attempt},
			"$set":  bson.M{"failure_reason": failureReason, "last_attempt_at": now, "updated_at": now},
			"$setOnInsert": bson.M{
				"event_id": event.EventID, "event": event, "retry_count": 0, "max_retries": h.maxRetries,
				"first_failure_at": now, "next_retry_at": now.Add(h.retryDelay), "status": DLQStatusPending,
				"created_at": now,
			},
		},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("grnoti/mongo: publish to dlq %s: %w", event.EventID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

// ClaimRetryableEvents claims events one FindOneAndUpdate per iteration
// (Mongo has no single-statement "claim up to N rows" equivalent to
// Postgres's SKIP LOCKED query). On a mid-loop error, already-claimed
// documents were durably transitioned to DLQStatusRetrying before the
// failure and are returned alongside the error rather than discarded — see
// DLQHandler.ClaimRetryableEvents's doc comment for why silently dropping
// them would orphan those events.
func (h *mongoDLQHandler) ClaimRetryableEvents(ctx context.Context, limit int) ([]*DLQEvent, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if limit <= 0 {
		limit = 10
	}
	now := time.Now().UTC()

	claimed := make([]*DLQEvent, 0, limit)
	for i := 0; i < limit; i++ {
		var doc mongoDLQDoc
		err := h.collection.FindOneAndUpdate(ctx,
			bson.M{"status": DLQStatusPending, "next_retry_at": bson.M{"$lte": now}},
			bson.M{"$set": bson.M{"status": DLQStatusRetrying, "updated_at": now}},
			options.FindOneAndUpdate().SetSort(bson.D{{Key: "next_retry_at", Value: 1}}).SetReturnDocument(options.After),
		).Decode(&doc)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}
			return claimed, fmt.Errorf("grnoti/mongo: claim retryable events: %w", errors.Join(err, ErrBackendUnavailable))
		}
		claimed = append(claimed, doc.toDLQEvent())
	}
	return claimed, nil
}

func (h *mongoDLQHandler) MarkRetried(ctx context.Context, eventID string, success bool, attemptErr error) error {
	if h.closed.Load() {
		return ErrClosed
	}
	now := time.Now().UTC()

	// Step 1: atomically increment retry_count, scoped to the claimed
	// ("retrying") state — the filter is what makes this safe: it only
	// succeeds for the one caller that actually holds the claim from
	// ClaimRetryableEvents, matching that method's single-claimant
	// contract (see its own doc comment).
	var doc mongoDLQDoc
	err := h.collection.FindOneAndUpdate(ctx,
		bson.M{"event_id": eventID, "status": DLQStatusRetrying},
		bson.M{"$inc": bson.M{"retry_count": 1}, "$set": bson.M{"last_attempt_at": now, "updated_at": now}},
		options.FindOneAndUpdate().SetReturnDocument(options.After),
	).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			if _, getErr := h.GetEventByID(ctx, eventID); getErr == ErrDLQEventNotFound {
				return ErrDLQEventNotFound
			}
			return ErrDLQEventNotClaimed
		}
		return fmt.Errorf("grnoti/mongo: mark retried %s: %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}

	errMsg := ""
	if attemptErr != nil {
		errMsg = attemptErr.Error()
	}
	setFields := bson.M{"updated_at": now}
	switch {
	case success:
		setFields["status"] = DLQStatusResolved
	case doc.RetryCount >= doc.MaxRetries:
		setFields["status"] = DLQStatusExhausted
	default:
		setFields["status"] = DLQStatusPending
		setFields["next_retry_at"] = now.Add(FullJitterBackoff(h.retryDelay, h.maxRetryDelay, doc.RetryCount))
	}

	attempt := DLQRetryAttempt{AttemptNumber: doc.RetryCount, AttemptedAt: now, Success: success, ErrorMessage: errMsg}
	// Step 2: finalize status/next_retry_at and record the attempt.
	if _, err := h.collection.UpdateOne(ctx,
		bson.M{"event_id": eventID},
		bson.M{"$set": setFields, "$push": bson.M{"attempt_history": attempt}},
	); err != nil {
		return fmt.Errorf("grnoti/mongo: mark retried %s (finalize): %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (h *mongoDLQHandler) GetEventByID(ctx context.Context, eventID string) (*DLQEvent, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	var doc mongoDLQDoc
	err := h.collection.FindOne(ctx, bson.M{"event_id": eventID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, ErrDLQEventNotFound
		}
		return nil, fmt.Errorf("grnoti/mongo: get event %s: %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}
	return doc.toDLQEvent(), nil
}

func (h *mongoDLQHandler) PurgeExpiredEvents(ctx context.Context, maxAge time.Duration) (int64, error) {
	if h.closed.Load() {
		return 0, ErrClosed
	}
	cutoff := time.Now().UTC().Add(-maxAge)
	res, err := h.collection.DeleteMany(ctx, bson.M{"$or": []bson.M{
		{"status": DLQStatusResolved},
		{"status": DLQStatusExhausted},
		{"created_at": bson.M{"$lt": cutoff}},
	}})
	if err != nil {
		return 0, fmt.Errorf("grnoti/mongo: purge expired events: %w", errors.Join(err, ErrBackendUnavailable))
	}
	return res.DeletedCount, nil
}

func (h *mongoDLQHandler) Close() error {
	var err error
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		err = h.client.Disconnect(context.Background())
		h.logger.Infof("grnoti/mongo: dlq handler closed")
	})
	return err
}
