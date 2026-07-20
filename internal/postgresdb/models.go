// File: internal/postgresdb/models.go

// versions:
//   sqlc v1.31.1

package postgresdb

import (
	"github.com/jackc/pgx/v5/pgtype"
)

type GrnotiDlq struct {
	EventID        string             `db:"event_id" json:"event_id"`
	EventData      []byte             `db:"event_data" json:"event_data"`
	FailureReason  string             `db:"failure_reason" json:"failure_reason"`
	RetryCount     int32              `db:"retry_count" json:"retry_count"`
	MaxRetries     int32              `db:"max_retries" json:"max_retries"`
	FirstFailureAt pgtype.Timestamptz `db:"first_failure_at" json:"first_failure_at"`
	LastAttemptAt  pgtype.Timestamptz `db:"last_attempt_at" json:"last_attempt_at"`
	NextRetryAt    pgtype.Timestamptz `db:"next_retry_at" json:"next_retry_at"`
	Status         string             `db:"status" json:"status"`
	AttemptHistory []byte             `db:"attempt_history" json:"attempt_history"`
	CreatedAt      pgtype.Timestamptz `db:"created_at" json:"created_at"`
	UpdatedAt      pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
}

type GrnotiExperiment struct {
	ID        string             `db:"id" json:"id"`
	Name      string             `db:"name" json:"name"`
	Variants  []byte             `db:"variants" json:"variants"`
	Enabled   bool               `db:"enabled" json:"enabled"`
	CreatedAt pgtype.Timestamptz `db:"created_at" json:"created_at"`
	UpdatedAt pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
}

type GrnotiPreference struct {
	UserID            string             `db:"user_id" json:"user_id"`
	GlobalEnabled     bool               `db:"global_enabled" json:"global_enabled"`
	QuietHoursEnabled bool               `db:"quiet_hours_enabled" json:"quiet_hours_enabled"`
	QuietHoursStart   string             `db:"quiet_hours_start" json:"quiet_hours_start"`
	QuietHoursEnd     string             `db:"quiet_hours_end" json:"quiet_hours_end"`
	Timezone          string             `db:"timezone" json:"timezone"`
	Locale            string             `db:"locale" json:"locale"`
	EventTypeSettings []byte             `db:"event_type_settings" json:"event_type_settings"`
	CreatedAt         pgtype.Timestamptz `db:"created_at" json:"created_at"`
	UpdatedAt         pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
}

type GrnotiToken struct {
	Token       string             `db:"token" json:"token"`
	Platform    string             `db:"platform" json:"platform"`
	UserID      string             `db:"user_id" json:"user_id"`
	AnonymousID string             `db:"anonymous_id" json:"anonymous_id"`
	DeviceID    string             `db:"device_id" json:"device_id"`
	AppVersion  string             `db:"app_version" json:"app_version"`
	IsActive    bool               `db:"is_active" json:"is_active"`
	CreatedAt   pgtype.Timestamptz `db:"created_at" json:"created_at"`
	UpdatedAt   pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
}
