// File: dlq.postgres.go

package grnoti

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gourdian25/grnoti/internal/postgresdb"
)

func dlqRowToDomain(r postgresdb.GrnotiDlq) (*DLQEvent, error) {
	var event Event
	if len(r.EventData) > 0 {
		if err := json.Unmarshal(r.EventData, &event); err != nil {
			return nil, fmt.Errorf("grnoti/postgres: decode event for %s: %w", r.EventID, err)
		}
	}
	var history []DLQRetryAttempt
	if len(r.AttemptHistory) > 0 {
		if err := json.Unmarshal(r.AttemptHistory, &history); err != nil {
			return nil, fmt.Errorf("grnoti/postgres: decode attempt history for %s: %w", r.EventID, err)
		}
	}
	return &DLQEvent{
		EventID: r.EventID, Event: event, FailureReason: r.FailureReason,
		RetryCount: int(r.RetryCount), MaxRetries: int(r.MaxRetries),
		FirstFailureAt: pgTime(r.FirstFailureAt), LastAttemptAt: pgTime(r.LastAttemptAt), NextRetryAt: pgTime(r.NextRetryAt),
		Status: DLQStatus(r.Status), AttemptHistory: history,
		CreatedAt: pgTime(r.CreatedAt), UpdatedAt: pgTime(r.UpdatedAt),
	}, nil
}

// PostgresDLQHandlerConfig configures a DLQHandler constructed by
// NewPostgresDLQHandler — the primary DLQ backend (see
// docs/plan/grnoti-plan.md §1.3, §6).
type PostgresDLQHandlerConfig struct {
	PostgresConfig
	MaxRetries    int           // defaults to 3
	RetryDelay    time.Duration // 0 is a valid, deliberate "immediately retry-eligible" choice — not defaulted
	MaxRetryDelay time.Duration // passed through to FullJitterBackoff as-is
}

type postgresDLQHandler struct {
	pool          *pgxpool.Pool
	queries       *postgresdb.Queries
	maxRetries    int
	retryDelay    time.Duration
	maxRetryDelay time.Duration
	logger        Logger
	ownsPool      bool

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ DLQHandler = (*postgresDLQHandler)(nil)

// NewPostgresDLQHandler connects per cfg.
//
// Claim semantics: ClaimRetryableEvents runs a single UPDATE statement
// whose subquery uses `SELECT ... FOR UPDATE SKIP LOCKED` to let N
// concurrent callers each claim a disjoint batch of pending events without
// contention — deliberately not graudit's pg_advisory_xact_lock (a single
// global serialization point, correct for graudit's one hash chain but
// wrong here, where claiming should be embarrassingly parallel across
// worker replicas). See docs/plan/grnoti-plan.md §1.3 and
// internal/postgresdb/queries/dlq.sql.
func NewPostgresDLQHandler(cfg PostgresDLQHandlerConfig) (DLQHandler, error) {
	pool, queries, ownsPool, err := connectPostgres(context.Background(), cfg.PostgresConfig, "DLQHandler")
	if err != nil {
		return nil, err
	}
	maxRetries := cfg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3
	}
	logger := OrNop(cfg.Logger)
	logger.Infof("grnoti/postgres: dlq handler connected")
	return &postgresDLQHandler{
		pool: pool, queries: queries,
		maxRetries: maxRetries, retryDelay: cfg.RetryDelay, maxRetryDelay: cfg.MaxRetryDelay,
		logger: logger, ownsPool: ownsPool,
	}, nil
}

func (h *postgresDLQHandler) PublishToDLQ(ctx context.Context, event Event, failureReason string) error {
	if h.closed.Load() {
		return ErrClosed
	}
	now := time.Now().UTC()

	eventData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("grnoti/postgres: encode event %s: %w", event.EventID, err)
	}
	attemptJSON, err := json.Marshal([]DLQRetryAttempt{{AttemptedAt: now, Success: false, ErrorMessage: failureReason}})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: encode attempt for %s: %w", event.EventID, err)
	}

	err = h.queries.UpsertDLQEvent(ctx, postgresdb.UpsertDLQEventParams{
		EventID: event.EventID, EventData: eventData, FailureReason: failureReason, MaxRetries: pgInt32(h.maxRetries),
		FirstFailureAt: pgTimestamptz(now), NextRetryAt: pgTimestamptz(now.Add(h.retryDelay)),
		Status: string(DLQStatusPending), AttemptHistory: attemptJSON,
	})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: publish to dlq %s: %w", event.EventID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (h *postgresDLQHandler) ClaimRetryableEvents(ctx context.Context, limit int) ([]*DLQEvent, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	if limit <= 0 {
		limit = 10
	}
	now := time.Now().UTC()

	rows, err := h.queries.ClaimRetryableEvents(ctx, postgresdb.ClaimRetryableEventsParams{
		NextRetryAt: pgTimestamptz(now), UpdatedAt: pgTimestamptz(now), Limit: pgInt32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: claim retryable events: %w", errors.Join(err, ErrBackendUnavailable))
	}
	claimed := make([]*DLQEvent, 0, len(rows))
	for _, r := range rows {
		e, err := dlqRowToDomain(r)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, e)
	}
	return claimed, nil
}

func (h *postgresDLQHandler) MarkRetried(ctx context.Context, eventID string, success bool, attemptErr error) error {
	if h.closed.Load() {
		return ErrClosed
	}
	now := time.Now().UTC()

	// Step 1: atomically increment retry_count, scoped to the claimed
	// ("retrying") state — see dlq.mongo.go's MarkRetried for the identical
	// reasoning (the WHERE clause is what makes this safe).
	row, err := h.queries.IncrementRetryCount(ctx, postgresdb.IncrementRetryCountParams{
		EventID: eventID, LastAttemptAt: pgTimestamptz(now),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if _, getErr := h.GetEventByID(ctx, eventID); getErr == ErrDLQEventNotFound {
				return ErrDLQEventNotFound
			}
			return ErrDLQEventNotClaimed
		}
		return fmt.Errorf("grnoti/postgres: mark retried %s: %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}

	errMsg := ""
	if attemptErr != nil {
		errMsg = attemptErr.Error()
	}
	retryCount := int(row.RetryCount)
	attemptJSON, jsonErr := json.Marshal([]DLQRetryAttempt{{AttemptNumber: retryCount, AttemptedAt: now, Success: success, ErrorMessage: errMsg}})
	if jsonErr != nil {
		return fmt.Errorf("grnoti/postgres: encode attempt for %s: %w", eventID, jsonErr)
	}

	// Step 2: finalize status/next_retry_at and record the attempt.
	switch {
	case success:
		err = h.queries.FinalizeRetryTerminal(ctx, postgresdb.FinalizeRetryTerminalParams{
			EventID: eventID, Status: string(DLQStatusResolved), UpdatedAt: pgTimestamptz(now), AttemptHistory: attemptJSON,
		})
	case retryCount >= int(row.MaxRetries):
		err = h.queries.FinalizeRetryTerminal(ctx, postgresdb.FinalizeRetryTerminalParams{
			EventID: eventID, Status: string(DLQStatusExhausted), UpdatedAt: pgTimestamptz(now), AttemptHistory: attemptJSON,
		})
	default:
		nextRetryAt := now.Add(FullJitterBackoff(h.retryDelay, h.maxRetryDelay, retryCount))
		err = h.queries.FinalizeRetryPending(ctx, postgresdb.FinalizeRetryPendingParams{
			EventID: eventID, NextRetryAt: pgTimestamptz(nextRetryAt), UpdatedAt: pgTimestamptz(now), AttemptHistory: attemptJSON,
		})
	}
	if err != nil {
		return fmt.Errorf("grnoti/postgres: mark retried %s (finalize): %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (h *postgresDLQHandler) GetEventByID(ctx context.Context, eventID string) (*DLQEvent, error) {
	if h.closed.Load() {
		return nil, ErrClosed
	}
	row, err := h.queries.GetDLQEventByID(ctx, eventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDLQEventNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get event %s: %w", eventID, errors.Join(err, ErrBackendUnavailable))
	}
	return dlqRowToDomain(row)
}

func (h *postgresDLQHandler) PurgeExpiredEvents(ctx context.Context, maxAge time.Duration) (int64, error) {
	if h.closed.Load() {
		return 0, ErrClosed
	}
	cutoff := time.Now().UTC().Add(-maxAge)
	purged, err := h.queries.PurgeExpiredEvents(ctx, pgTimestamptz(cutoff))
	if err != nil {
		return 0, fmt.Errorf("grnoti/postgres: purge expired events: %w", errors.Join(err, ErrBackendUnavailable))
	}
	return purged, nil
}

func (h *postgresDLQHandler) Close() error {
	h.closeOnce.Do(func() {
		h.closed.Store(true)
		if h.ownsPool {
			h.pool.Close()
		}
		h.logger.Infof("grnoti/postgres: dlq handler closed")
	})
	return nil
}
