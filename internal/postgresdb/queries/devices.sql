-- File: internal/postgresdb/queries/devices.sql

-- name: GetActiveDevicesByUserID :many
SELECT * FROM grnoti_devices WHERE user_id = $1 AND is_active = true;

-- name: GetActiveDevicesByUserIDs :many
SELECT * FROM grnoti_devices WHERE user_id = ANY(@user_ids::varchar[]) AND is_active = true;

-- name: GetActiveDevicesByAnonymousID :many
SELECT * FROM grnoti_devices WHERE anonymous_id = $1 AND is_active = true;

-- name: MarkDeviceInvalid :execrows
UPDATE grnoti_devices SET is_active = false, updated_at = $2 WHERE fid = $1;

-- name: UpsertDevice :exec
INSERT INTO grnoti_devices (fid, platform, user_id, anonymous_id, device_id, app_version, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, true, $7, $7)
ON CONFLICT (fid) DO UPDATE SET
    platform = EXCLUDED.platform,
    user_id = EXCLUDED.user_id,
    anonymous_id = EXCLUDED.anonymous_id,
    device_id = EXCLUDED.device_id,
    app_version = EXCLUDED.app_version,
    is_active = true,
    updated_at = EXCLUDED.updated_at;

-- name: DeleteDevice :execrows
DELETE FROM grnoti_devices WHERE fid = $1;
