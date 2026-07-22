-- File: internal/postgresdb/queries/preferences.sql

-- name: GetPreferences :one
SELECT * FROM grnoti_preferences WHERE user_id = $1;

-- name: UpsertPreferences :exec
INSERT INTO grnoti_preferences (user_id, global_enabled, quiet_hours_enabled, quiet_hours_start, quiet_hours_end, timezone, locale, event_type_settings, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
ON CONFLICT (user_id) DO UPDATE SET
    global_enabled = EXCLUDED.global_enabled,
    quiet_hours_enabled = EXCLUDED.quiet_hours_enabled,
    quiet_hours_start = EXCLUDED.quiet_hours_start,
    quiet_hours_end = EXCLUDED.quiet_hours_end,
    timezone = EXCLUDED.timezone,
    locale = EXCLUDED.locale,
    event_type_settings = EXCLUDED.event_type_settings,
    updated_at = EXCLUDED.updated_at;
