-- File: internal/postgresdb/queries/experiments.sql

-- name: CreateExperiment :exec
INSERT INTO grnoti_experiments (id, name, variants, enabled, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $5);

-- name: GetExperiment :one
SELECT * FROM grnoti_experiments WHERE id = $1;

-- name: UpdateExperiment :execrows
UPDATE grnoti_experiments SET name = $2, variants = $3, enabled = $4, updated_at = $5 WHERE id = $1;

-- name: DeleteExperiment :exec
DELETE FROM grnoti_experiments WHERE id = $1;

-- name: ListExperiments :many
SELECT * FROM grnoti_experiments ORDER BY id ASC;
