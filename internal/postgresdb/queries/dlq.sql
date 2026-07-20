-- File: internal/postgresdb/queries/dlq.sql

-- name: UpsertDLQEvent :exec
-- A single atomic upsert handles both "new failure" (insert) and "another
-- failure for an event already pending/retrying" (update, via jsonb ||
-- concatenation onto the existing history) in one write path — see
-- docs/plan/grnoti-plan.md §3.5 for the defect this design fixes (the
-- reference implementation had two separate, uncoordinated writers for
-- these two cases).
INSERT INTO grnoti_dlq (event_id, event_data, failure_reason, retry_count, max_retries, first_failure_at, last_attempt_at, next_retry_at, status, attempt_history, created_at, updated_at)
VALUES ($1, $2, $3, 0, $4, $5, $5, $6, $7, $8, $5, $5)
ON CONFLICT (event_id) DO UPDATE SET
    failure_reason = EXCLUDED.failure_reason,
    last_attempt_at = EXCLUDED.last_attempt_at,
    updated_at = EXCLUDED.updated_at,
    attempt_history = grnoti_dlq.attempt_history || EXCLUDED.attempt_history;

-- name: ClaimRetryableEvents :many
-- Single-statement atomic claim: the inner SELECT ... FOR UPDATE SKIP
-- LOCKED lets N concurrent callers each lock a disjoint set of candidate
-- rows without blocking each other, and the outer UPDATE...RETURNING
-- transitions exactly those locked rows in the same statement — no
-- explicit transaction wrapper needed, a single statement is already
-- atomic. Deliberately not graudit's pg_advisory_xact_lock (a single
-- global serialization point, wrong here — see docs/plan/grnoti-plan.md
-- §1.3).
UPDATE grnoti_dlq SET status = 'retrying', updated_at = $2
WHERE event_id IN (
    SELECT candidate.event_id FROM grnoti_dlq AS candidate
    WHERE candidate.status = 'pending' AND candidate.next_retry_at <= $1
    ORDER BY candidate.next_retry_at ASC
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
RETURNING *;

-- name: IncrementRetryCount :one
-- Step 1 of MarkRetried: atomically increment retry_count, scoped to the
-- claimed ("retrying") state — the WHERE clause is what makes this safe,
-- matching dlq.mongo.go's identical two-step design. Returns no row if
-- eventID doesn't exist or isn't currently claimed; the caller
-- distinguishes those two cases via a follow-up GetDLQEventByID.
UPDATE grnoti_dlq SET retry_count = retry_count + 1, last_attempt_at = $2, updated_at = $2
WHERE event_id = $1 AND status = 'retrying'
RETURNING *;

-- name: FinalizeRetryPending :exec
-- Step 2 of MarkRetried when the event goes back to pending (a retryable
-- failure, retries not yet exhausted).
UPDATE grnoti_dlq SET status = 'pending', next_retry_at = $2, updated_at = $3, attempt_history = attempt_history || $4
WHERE event_id = $1;

-- name: FinalizeRetryTerminal :exec
-- Step 2 of MarkRetried when the event reaches a terminal state (resolved
-- on success, exhausted after MaxRetries).
UPDATE grnoti_dlq SET status = $2, updated_at = $3, attempt_history = attempt_history || $4
WHERE event_id = $1;

-- name: GetDLQEventByID :one
SELECT * FROM grnoti_dlq WHERE event_id = $1;

-- name: PurgeExpiredEvents :execrows
DELETE FROM grnoti_dlq WHERE status IN ('resolved', 'exhausted') OR created_at < $1;
