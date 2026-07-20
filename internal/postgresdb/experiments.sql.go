// File: internal/postgresdb/experiments.sql.go

// versions:
//   sqlc v1.31.1
// source: experiments.sql

package postgresdb

import (
	"context"

	"github.com/jackc/pgx/v5/pgtype"
)

const createExperiment = `-- name: CreateExperiment :exec

INSERT INTO grnoti_experiments (id, name, variants, enabled, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $5)
`

type CreateExperimentParams struct {
	ID        string             `db:"id" json:"id"`
	Name      string             `db:"name" json:"name"`
	Variants  []byte             `db:"variants" json:"variants"`
	Enabled   bool               `db:"enabled" json:"enabled"`
	CreatedAt pgtype.Timestamptz `db:"created_at" json:"created_at"`
}

// File: internal/postgresdb/queries/experiments.sql
func (q *Queries) CreateExperiment(ctx context.Context, arg CreateExperimentParams) error {
	_, err := q.db.Exec(ctx, createExperiment,
		arg.ID,
		arg.Name,
		arg.Variants,
		arg.Enabled,
		arg.CreatedAt,
	)
	return err
}

const deleteExperiment = `-- name: DeleteExperiment :exec
DELETE FROM grnoti_experiments WHERE id = $1
`

func (q *Queries) DeleteExperiment(ctx context.Context, id string) error {
	_, err := q.db.Exec(ctx, deleteExperiment, id)
	return err
}

const getExperiment = `-- name: GetExperiment :one
SELECT id, name, variants, enabled, created_at, updated_at FROM grnoti_experiments WHERE id = $1
`

func (q *Queries) GetExperiment(ctx context.Context, id string) (GrnotiExperiment, error) {
	row := q.db.QueryRow(ctx, getExperiment, id)
	var i GrnotiExperiment
	err := row.Scan(
		&i.ID,
		&i.Name,
		&i.Variants,
		&i.Enabled,
		&i.CreatedAt,
		&i.UpdatedAt,
	)
	return i, err
}

const listExperiments = `-- name: ListExperiments :many
SELECT id, name, variants, enabled, created_at, updated_at FROM grnoti_experiments ORDER BY id ASC
`

func (q *Queries) ListExperiments(ctx context.Context) ([]GrnotiExperiment, error) {
	rows, err := q.db.Query(ctx, listExperiments)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []GrnotiExperiment
	for rows.Next() {
		var i GrnotiExperiment
		if err := rows.Scan(
			&i.ID,
			&i.Name,
			&i.Variants,
			&i.Enabled,
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

const updateExperiment = `-- name: UpdateExperiment :execrows
UPDATE grnoti_experiments SET name = $2, variants = $3, enabled = $4, updated_at = $5 WHERE id = $1
`

type UpdateExperimentParams struct {
	ID        string             `db:"id" json:"id"`
	Name      string             `db:"name" json:"name"`
	Variants  []byte             `db:"variants" json:"variants"`
	Enabled   bool               `db:"enabled" json:"enabled"`
	UpdatedAt pgtype.Timestamptz `db:"updated_at" json:"updated_at"`
}

func (q *Queries) UpdateExperiment(ctx context.Context, arg UpdateExperimentParams) (int64, error) {
	result, err := q.db.Exec(ctx, updateExperiment,
		arg.ID,
		arg.Name,
		arg.Variants,
		arg.Enabled,
		arg.UpdatedAt,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}
