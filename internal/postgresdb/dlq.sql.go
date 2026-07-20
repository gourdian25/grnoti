// File: internal/postgresdb/dlq.sql.go

// versions:
//   sqlc v1.31.1
// source: dlq.sql

package postgresdb

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const claimRetryableEvents = `-- name: ClaimRetryableEvents :many
UPDATE grnoti_dlq SET status = 'retrying', updated_at = $2
WHERE event_id IN (
    SELECT candidate.event_id FROM grnoti_dlq AS candidate
    WHERE candidate.status = 'pending' AND candidate.next_retry_at <= $1
    ORDER BY candidate.next_retry_at ASC
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING event_id, event_data, failure_reason, retry_count, max_retries, first_failure_at, last_attempt_at, next_retry_at, status, attempt_history, created_at, updated_at
`

type ClaimRetryableEventsParams struct {
	NextRetryAt pgtype.Timestamptz `db:"next_retry_at" json:"next_retry_at"`
	UpdatedAt   pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
	Limit       int32              `db:"limit" json:"limit"`
}

// Single-statement atomic claim: the inner SELECT ... FOR UPDATE SKIP
// LOCKED lets N concurrent callers each lock a disjoint set of candidate
// rows without blocking each other, and the outer UPDATE...RETURNING
// transitions exactly those locked rows in the same statement — no
// explicit transaction wrapper needed, a single statement is already
// atomic. Deliberately not graudit's pg_advisory_xact_lock (a single
// global serialization point, wrong here — see docs/plan/grnoti-plan.md
// §1.3).
func (q *Queries) ClaimRetryableEvents(ctx context.Context, arg ClaimRetryableEventsParams) ([]GrnotiDlq, error) {
	rows, err := q.db.Query(ctx, claimRetryableEvents, arg.NextRetryAt, arg.UpdatedAt, arg.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GrnotiDlq
	for rows.Next() {
		var i GrnotiDlq
		if err := rows.Scan(
			&i.EventID,
			&i.EventData,
			&i.FailureReason,
			&i.RetryCount,
			&i.MaxRetries,
			&i.FirstFailureAt,
			&i.LastAttemptAt,
			&i.NextRetryAt,
			&i.Status,
			&i.AttemptHistory,
			&i.CreatedAt,
			&i.UpdatedAt,
		); err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

const finalizeRetryPending = `-- name: FinalizeRetryPending :exec
UPDATE grnoti_dlq SET status = 'pending', next_retry_at = $2, updated_at = $3, attempt_history = attempt_history || $4
WHERE event_id = $1
`

type FinalizeRetryPendingParams struct {
	EventID        string             `db:"event_id" json:"event_id"`
	NextRetryAt    pgtype.Timestamptz `db:"next_retry_at" json:"next_retry_at"`
	UpdatedAt      pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
	AttemptHistory []byte             `db:"attempt_history" json:"attempt_history"`
}

// Step 2 of MarkRetried when the event goes back to pending (a retryable
// failure, retries not yet exhausted).
func (q *Queries) FinalizeRetryPending(ctx context.Context, arg FinalizeRetryPendingParams) error {
	_, err := q.db.Exec(ctx, finalizeRetryPending,
		arg.EventID,
		arg.NextRetryAt,
		arg.UpdatedAt,
		arg.AttemptHistory,
	)
	return err
}

const finalizeRetryTerminal = `-- name: FinalizeRetryTerminal :exec
UPDATE grnoti_dlq SET status = $2, updated_at = $3, attempt_history = attempt_history || $4
WHERE event_id = $1
`

type FinalizeRetryTerminalParams struct {
	EventID        string             `db:"event_id" json:"event_id"`
	Status         string             `db:"status" json:"status"`
	UpdatedAt      pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
	AttemptHistory []byte             `db:"attempt_history" json:"attempt_history"`
}

// Step 2 of MarkRetried when the event reaches a terminal state (resolved
// on success, exhausted after MaxRetries).
func (q *Queries) FinalizeRetryTerminal(ctx context.Context, arg FinalizeRetryTerminalParams) error {
	_, err := q.db.Exec(ctx, finalizeRetryTerminal,
		arg.EventID,
		arg.Status,
		arg.UpdatedAt,
		arg.AttemptHistory,
	)
	return err
}

const getDLQEventByID = `-- name: GetDLQEventByID :one
SELECT event_id, event_data, failure_reason, retry_count, max_retries, first_failure_at, last_attempt_at, next_retry_at, status, attempt_history, created_at, updated_at FROM grnoti_dlq WHERE event_id = $1
`

func (q *Queries) GetDLQEventByID(ctx context.Context, eventID string) (GrnotiDlq, error) {
	row := q.db.QueryRow(ctx, getDLQEventByID, eventID)
	var i GrnotiDlq
	err := row.Scan(
		&i.EventID,
		&i.EventData,
		&i.FailureReason,
		&i.RetryCount,
		&i.MaxRetries,
		&i.FirstFailureAt,
		&i.LastAttemptAt,
		&i.NextRetryAt,
		&i.Status,
		&i.AttemptHistory,
		&i.CreatedAt,
		&i.UpdatedAt,
	)
	return i, err
}

const incrementRetryCount = `-- name: IncrementRetryCount :one
UPDATE grnoti_dlq SET retry_count = retry_count + 1, last_attempt_at = $2, updated_at = $2
WHERE event_id = $1 AND status = 'retrying'
RETURNING event_id, event_data, failure_reason, retry_count, max_retries, first_failure_at, last_attempt_at, next_retry_at, status, attempt_history, created_at, updated_at
`

type IncrementRetryCountParams struct {
	EventID       string             `db:"event_id" json:"event_id"`
	LastAttemptAt pgtype.Timestamptz `db:"last_attempt_at" json:"last_attempt_at"`
}

// Step 1 of MarkRetried: atomically increment retry_count, scoped to the
// claimed ("retrying") state — the WHERE clause is what makes this safe,
// matching dlq.mongo.go's identical two-step design. Returns no row if
// eventID doesn't exist or isn't currently claimed; the caller
// distinguishes those two cases via a follow-up GetDLQEventByID.
func (q *Queries) IncrementRetryCount(ctx context.Context, arg IncrementRetryCountParams) (GrnotiDlq, error) {
	row := q.db.QueryRow(ctx, incrementRetryCount, arg.EventID, arg.LastAttemptAt)
	var i GrnotiDlq
	err := row.Scan(
		&i.EventID,
		&i.EventData,
		&i.FailureReason,
		&i.RetryCount,
		&i.MaxRetries,
		&i.FirstFailureAt,
		&i.LastAttemptAt,
		&i.NextRetryAt,
		&i.Status,
		&i.AttemptHistory,
		&i.CreatedAt,
		&i.UpdatedAt,
	)
	return i, err
}

const purgeExpiredEvents = `-- name: PurgeExpiredEvents :execrows
DELETE FROM grnoti_dlq WHERE status IN ('resolved', 'exhausted') OR created_at < $1
`

func (q *Queries) PurgeExpiredEvents(ctx context.Context, createdAt pgtype.Timestamptz) (int64, error) {
	result, err := q.db.Exec(ctx, purgeExpiredEvents, createdAt)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const upsertDLQEvent = `-- name: UpsertDLQEvent :exec

INSERT INTO grnoti_dlq (event_id, event_data, failure_reason, retry_count, max_retries, first_failure_at, last_attempt_at, next_retry_at, status, attempt_history, created_at, updated_at)
VALUES ($1, $2, $3, 0, $4, $5, $5, $6, $7, $8, $5, $5)
ON CONFLICT (event_id) DO UPDATE SET
    failure_reason = EXCLUDED.failure_reason,
    last_attempt_at = EXCLUDED.last_attempt_at,
    updated_at = EXCLUDED.updated_at,
    attempt_history = grnoti_dlq.attempt_history || EXCLUDED.attempt_history
`

type UpsertDLQEventParams struct {
	EventID        string             `db:"event_id" json:"event_id"`
	EventData      []byte             `db:"event_data" json:"event_data"`
	FailureReason  string             `db:"failure_reason" json:"failure_reason"`
	MaxRetries     int32              `db:"max_retries" json:"max_retries"`
	FirstFailureAt pgtype.Timestamptz `db:"first_failure_at" json:"first_failure_at"`
	NextRetryAt    pgtype.Timestamptz `db:"next_retry_at" json:"next_retry_at"`
	Status         string             `db:"status" json:"status"`
	AttemptHistory []byte             `db:"attempt_history" json:"attempt_history"`
}

// File: internal/postgresdb/queries/dlq.sql
// A single atomic upsert handles both "new failure" (insert) and "another
// failure for an event already pending/retrying" (update, via jsonb ||
// concatenation onto the existing history) in one write path — see
// docs/plan/grnoti-plan.md §3.5 for the defect this design fixes (the
// reference implementation had two separate, uncoordinated writers for
// these two cases).
func (q *Queries) UpsertDLQEvent(ctx context.Context, arg UpsertDLQEventParams) error {
	_, err := q.db.Exec(ctx, upsertDLQEvent,
		arg.EventID,
		arg.EventData,
		arg.FailureReason,
		arg.MaxRetries,
		arg.FirstFailureAt,
		arg.NextRetryAt,
		arg.Status,
		arg.AttemptHistory,
	)
	return err
}
