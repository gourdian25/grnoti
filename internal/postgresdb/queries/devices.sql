-- File: internal/postgresdb/queries/tokens.sql

-- name: GetActiveTokensByUserID :many
SELECT * FROM grnoti_tokens WHERE user_id = $1 AND is_active = true;

-- name: GetActiveTokensByUserIDs :many
SELECT * FROM grnoti_tokens WHERE user_id = ANY(@user_ids::varchar[]) AND is_active = true;

-- name: GetActiveTokensByAnonymousID :many
SELECT * FROM grnoti_tokens WHERE anonymous_id = $1 AND is_active = true;

-- name: MarkTokenInvalid :execrows
UPDATE grnoti_tokens SET is_active = false, updated_at = $2 WHERE token = $1;

-- name: UpsertToken :exec
INSERT INTO grnoti_tokens (token, platform, user_id, anonymous_id, device_id, app_version, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, true, $7, $7)
ON CONFLICT (token) DO UPDATE SET
    platform = EXCLUDED.platform,
    user_id = EXCLUDED.user_id,
    anonymous_id = EXCLUDED.anonymous_id,
    device_id = EXCLUDED.device_id,
    app_version = EXCLUDED.app_version,
    is_active = true,
    updated_at = EXCLUDED.updated_at;

-- name: DeleteToken :execrows
DELETE FROM grnoti_tokens WHERE token = $1;
