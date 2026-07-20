// File: internal/postgresdb/querier.go

// versions:
//   sqlc v1.31.1

package postgresdb

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

type Querier interface {
	// Single-statement atomic claim: the inner SELECT ... FOR UPDATE SKIP
	// LOCKED lets N concurrent callers each lock a disjoint set of candidate
	// rows without blocking each other, and the outer UPDATE...RETURNING
	// transitions exactly those locked rows in the same statement — no
	// explicit transaction wrapper needed, a single statement is already
	// atomic. Deliberately not graudit's pg_advisory_xact_lock (a single
	// global serialization point, wrong here — see docs/plan/grnoti-plan.md
	// §1.3).
	ClaimRetryableEvents(ctx context.Context, arg ClaimRetryableEventsParams) ([]GrnotiDlq, error)
	// File: internal/postgresdb/queries/experiments.sql
	CreateExperiment(ctx context.Context, arg CreateExperimentParams) error
	DeleteExperiment(ctx context.Context, id string) error
	DeleteToken(ctx context.Context, token string) (int64, error)
	// Step 2 of MarkRetried when the event goes back to pending (a retryable
	// failure, retries not yet exhausted).
	FinalizeRetryPending(ctx context.Context, arg FinalizeRetryPendingParams) error
	// Step 2 of MarkRetried when the event reaches a terminal state (resolved
	// on success, exhausted after MaxRetries).
	FinalizeRetryTerminal(ctx context.Context, arg FinalizeRetryTerminalParams) error
	GetActiveTokensByAnonymousID(ctx context.Context, anonymousID string) ([]GrnotiToken, error)
	// File: internal/postgresdb/queries/tokens.sql
	GetActiveTokensByUserID(ctx context.Context, userID string) ([]GrnotiToken, error)
	GetActiveTokensByUserIDs(ctx context.Context, userIds []string) ([]GrnotiToken, error)
	GetDLQEventByID(ctx context.Context, eventID string) (GrnotiDlq, error)
	GetExperiment(ctx context.Context, id string) (GrnotiExperiment, error)
	// File: internal/postgresdb/queries/preferences.sql
	GetPreferences(ctx context.Context, userID string) (GrnotiPreference, error)
	// Step 1 of MarkRetried: atomically increment retry_count, scoped to the
	// claimed ("retrying") state — the WHERE clause is what makes this safe,
	// matching dlq.mongo.go's identical two-step design. Returns no row if
	// eventID doesn't exist or isn't currently claimed; the caller
	// distinguishes those two cases via a follow-up GetDLQEventByID.
	IncrementRetryCount(ctx context.Context, arg IncrementRetryCountParams) (GrnotiDlq, error)
	ListExperiments(ctx context.Context) ([]GrnotiExperiment, error)
	MarkTokenInvalid(ctx context.Context, arg MarkTokenInvalidParams) (int64, error)
	PurgeExpiredEvents(ctx context.Context, createdAt pgtype.Timestamptz) (int64, error)
	UpdateExperiment(ctx context.Context, arg UpdateExperimentParams) (int64, error)
	// File: internal/postgresdb/queries/dlq.sql
	// A single atomic upsert handles both "new failure" (insert) and "another
	// failure for an event already pending/retrying" (update, via jsonb ||
	// concatenation onto the existing history) in one write path — see
	// docs/plan/grnoti-plan.md §3.5 for the defect this design fixes (the
	// reference implementation had two separate, uncoordinated writers for
	// these two cases).
	UpsertDLQEvent(ctx context.Context, arg UpsertDLQEventParams) error
	UpsertPreferences(ctx context.Context, arg UpsertPreferencesParams) error
	UpsertToken(ctx context.Context, arg UpsertTokenParams) error
}

var _ Querier = (*Queries)(nil)
