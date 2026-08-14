-- File: internal/postgresdb/schema.sql
--
-- Schema for grnoti's PostgreSQL-backed stores. Returned as text by
-- SchemaSQL() (postgres.go) for the consuming application to apply
-- through its own project's migration tool (golang-migrate, Flyway, a
-- plain SQL file in CI, ...) -- grnoti itself never applies this; none of
-- NewPostgresTokenStore/NewPostgresPreferencesStore/
-- NewPostgresExperimentStore/NewPostgresDLQHandler assume anything beyond
-- "the tables already exist". CREATE TABLE IF NOT EXISTS is used so
-- applying it more than once (e.g. re-running your own migration) is
-- harmless, but that alone does not remove the need for a CREATE-capable
-- role to run it at least once -- see docs/postgres.md for the
-- least-privilege-runtime-role rationale and the full pattern. grnoti's
-- own test setup applies this same text directly (applyPostgresSchema in
-- postgres.go, serialized by a Postgres advisory lock) so its test suite
-- stays self-contained without a real migration tool.

CREATE TABLE IF NOT EXISTS grnoti_tokens (
    token        VARCHAR(512) PRIMARY KEY,
    platform     VARCHAR(16)  NOT NULL,
    user_id      VARCHAR(255) NOT NULL DEFAULT '',
    anonymous_id VARCHAR(255) NOT NULL DEFAULT '',
    device_id    VARCHAR(255) NOT NULL DEFAULT '',
    app_version  VARCHAR(64)  NOT NULL DEFAULT '',
    is_active    BOOLEAN      NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL,
    updated_at   TIMESTAMPTZ  NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_grnoti_tokens_user ON grnoti_tokens (user_id, is_active);
CREATE INDEX IF NOT EXISTS idx_grnoti_tokens_anon ON grnoti_tokens (anonymous_id, is_active);

CREATE TABLE IF NOT EXISTS grnoti_preferences (
    user_id             VARCHAR(255) PRIMARY KEY,
    global_enabled      BOOLEAN      NOT NULL,
    quiet_hours_enabled BOOLEAN      NOT NULL,
    quiet_hours_start   VARCHAR(5)   NOT NULL DEFAULT '',
    quiet_hours_end     VARCHAR(5)   NOT NULL DEFAULT '',
    timezone            VARCHAR(64)  NOT NULL DEFAULT '',
    locale              VARCHAR(16)  NOT NULL DEFAULT '',
    event_type_settings JSONB        NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ  NOT NULL,
    updated_at          TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS grnoti_experiments (
    id         VARCHAR(255) PRIMARY KEY,
    name       VARCHAR(255) NOT NULL DEFAULT '',
    variants   JSONB        NOT NULL DEFAULT '[]',
    enabled    BOOLEAN      NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL,
    updated_at TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS grnoti_dlq (
    event_id         VARCHAR(255) PRIMARY KEY,
    event_data       JSONB        NOT NULL,
    failure_reason   TEXT         NOT NULL DEFAULT '',
    retry_count      INT          NOT NULL DEFAULT 0,
    max_retries      INT          NOT NULL,
    first_failure_at TIMESTAMPTZ  NOT NULL,
    last_attempt_at  TIMESTAMPTZ  NOT NULL,
    next_retry_at    TIMESTAMPTZ  NOT NULL,
    status           VARCHAR(32)  NOT NULL,
    attempt_history  JSONB        NOT NULL DEFAULT '[]',
    created_at       TIMESTAMPTZ  NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_grnoti_dlq_status_next_retry ON grnoti_dlq (status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_grnoti_dlq_created_at ON grnoti_dlq (created_at);
