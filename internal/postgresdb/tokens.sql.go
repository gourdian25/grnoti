// File: internal/postgresdb/tokens.sql.go

// versions:
//   sqlc v1.31.1
// source: tokens.sql

package postgresdb

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const deleteToken = `-- name: DeleteToken :execrows
DELETE FROM grnoti_tokens WHERE token = $1
`

func (q *Queries) DeleteToken(ctx context.Context, token string) (int64, error) {
	result, err := q.db.Exec(ctx, deleteToken, token)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const getActiveTokensByAnonymousID = `-- name: GetActiveTokensByAnonymousID :many
SELECT token, platform, user_id, anonymous_id, device_id, app_version, is_active, created_at, updated_at FROM grnoti_tokens WHERE anonymous_id = $1 AND is_active = true
`

func (q *Queries) GetActiveTokensByAnonymousID(ctx context.Context, anonymousID string) ([]GrnotiToken, error) {
	rows, err := q.db.Query(ctx, getActiveTokensByAnonymousID, anonymousID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GrnotiToken
	for rows.Next() {
		var i GrnotiToken
		if err := rows.Scan(
			&i.Token,
			&i.Platform,
			&i.UserID,
			&i.AnonymousID,
			&i.DeviceID,
			&i.AppVersion,
			&i.IsActive,
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

const getActiveTokensByUserID = `-- name: GetActiveTokensByUserID :many

SELECT token, platform, user_id, anonymous_id, device_id, app_version, is_active, created_at, updated_at FROM grnoti_tokens WHERE user_id = $1 AND is_active = true
`

// File: internal/postgresdb/queries/tokens.sql
func (q *Queries) GetActiveTokensByUserID(ctx context.Context, userID string) ([]GrnotiToken, error) {
	rows, err := q.db.Query(ctx, getActiveTokensByUserID, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GrnotiToken
	for rows.Next() {
		var i GrnotiToken
		if err := rows.Scan(
			&i.Token,
			&i.Platform,
			&i.UserID,
			&i.AnonymousID,
			&i.DeviceID,
			&i.AppVersion,
			&i.IsActive,
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

const getActiveTokensByUserIDs = `-- name: GetActiveTokensByUserIDs :many
SELECT token, platform, user_id, anonymous_id, device_id, app_version, is_active, created_at, updated_at FROM grnoti_tokens WHERE user_id = ANY($1::varchar[]) AND is_active = true
`

func (q *Queries) GetActiveTokensByUserIDs(ctx context.Context, userIds []string) ([]GrnotiToken, error) {
	rows, err := q.db.Query(ctx, getActiveTokensByUserIDs, userIds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GrnotiToken
	for rows.Next() {
		var i GrnotiToken
		if err := rows.Scan(
			&i.Token,
			&i.Platform,
			&i.UserID,
			&i.AnonymousID,
			&i.DeviceID,
			&i.AppVersion,
			&i.IsActive,
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

const markTokenInvalid = `-- name: MarkTokenInvalid :execrows
UPDATE grnoti_tokens SET is_active = false, updated_at = $2 WHERE token = $1
`

type MarkTokenInvalidParams struct {
	Token     string             `db:"token" json:"token"`
	UpdatedAt pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
}

func (q *Queries) MarkTokenInvalid(ctx context.Context, arg MarkTokenInvalidParams) (int64, error) {
	result, err := q.db.Exec(ctx, markTokenInvalid, arg.Token, arg.UpdatedAt)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

const upsertToken = `-- name: UpsertToken :exec
INSERT INTO grnoti_tokens (token, platform, user_id, anonymous_id, device_id, app_version, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, true, $7, $7)
ON CONFLICT (token) DO UPDATE SET
    platform = EXCLUDED.platform,
    user_id = EXCLUDED.user_id,
    anonymous_id = EXCLUDED.anonymous_id,
    device_id = EXCLUDED.device_id,
    app_version = EXCLUDED.app_version,
    is_active = true,
    updated_at = EXCLUDED.updated_at
`

type UpsertTokenParams struct {
	Token       string             `db:"token" json:"token"`
	Platform    string             `db:"platform" json:"platform"`
	UserID      string             `db:"user_id" json:"user_id"`
	AnonymousID string             `db:"anonymous_id" json:"anonymous_id"`
	DeviceID    string             `db:"device_id" json:"device_id"`
	AppVersion  string             `db:"app_version" json:"app_version"`
	CreatedAt   pgtype.Timestamptz `db:"created_at" json:"created_at"`
}

func (q *Queries) UpsertToken(ctx context.Context, arg UpsertTokenParams) error {
	_, err := q.db.Exec(ctx, upsertToken,
		arg.Token,
		arg.Platform,
		arg.UserID,
		arg.AnonymousID,
		arg.DeviceID,
		arg.AppVersion,
		arg.CreatedAt,
	)
	return err
}
