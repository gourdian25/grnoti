// File: experimentstore.postgres.go

package grnoti

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gourdian25/grnoti/internal/postgresdb"
)

// pgUniqueViolation is Postgres's SQLSTATE code for a unique-constraint
// violation (23505).
const pgUniqueViolation = "23505"

func experimentRowToDomain(r postgresdb.GrnotiExperiment) (*Experiment, error) {
	var variants []ExperimentVariant
	if len(r.Variants) > 0 {
		if err := json.Unmarshal(r.Variants, &variants); err != nil {
			return nil, fmt.Errorf("grnoti/postgres: decode variants for %s: %w", r.ID, err)
		}
	}
	return &Experiment{
		ID: r.ID, Name: r.Name, Variants: variants, Enabled: r.Enabled,
		CreatedAt: pgTime(r.CreatedAt), UpdatedAt: pgTime(r.UpdatedAt),
	}, nil
}

type postgresExperimentStore struct {
	pool     *pgxpool.Pool
	queries  *postgresdb.Queries
	logger   Logger
	ownsPool bool

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ ExperimentStore = (*postgresExperimentStore)(nil)

// NewPostgresExperimentStore connects per cfg — CRUD storage for
// Experiment definitions (see docs/plan/grnoti-plan.md §6; the
// deterministic assignment algorithm itself lives in experiment.go/
// cache.experiment.go, not here).
func NewPostgresExperimentStore(cfg PostgresConfig) (ExperimentStore, error) {
	pool, queries, ownsPool, err := connectPostgres(context.Background(), cfg, "ExperimentStore")
	if err != nil {
		return nil, err
	}
	logger := OrNop(cfg.Logger)
	logger.Info("grnoti/postgres: experiment store connected")
	return &postgresExperimentStore{pool: pool, queries: queries, logger: logger, ownsPool: ownsPool}, nil
}

func (s *postgresExperimentStore) CreateExperiment(ctx context.Context, experiment *Experiment) error {
	if s.closed.Load() {
		return ErrClosed
	}
	variants, err := json.Marshal(experiment.Variants)
	if err != nil {
		return fmt.Errorf("grnoti/postgres: encode variants for %s: %w", experiment.ID, err)
	}
	now := time.Now().UTC()

	err = s.queries.CreateExperiment(ctx, postgresdb.CreateExperimentParams{
		ID: experiment.ID, Name: experiment.Name, Variants: variants, Enabled: experiment.Enabled,
		CreatedAt: pgTimestamptz(now),
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation {
			return ErrExperimentAlreadyExists
		}
		return fmt.Errorf("grnoti/postgres: create experiment %s: %w", experiment.ID, errors.Join(err, ErrBackendUnavailable))
	}
	experiment.CreatedAt = now
	experiment.UpdatedAt = now
	return nil
}

func (s *postgresExperimentStore) GetExperiment(ctx context.Context, experimentID string) (*Experiment, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	row, err := s.queries.GetExperiment(ctx, experimentID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrExperimentNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get experiment %s: %w", experimentID, errors.Join(err, ErrBackendUnavailable))
	}
	return experimentRowToDomain(row)
}

func (s *postgresExperimentStore) UpdateExperiment(ctx context.Context, experiment *Experiment) error {
	if s.closed.Load() {
		return ErrClosed
	}
	variants, err := json.Marshal(experiment.Variants)
	if err != nil {
		return fmt.Errorf("grnoti/postgres: encode variants for %s: %w", experiment.ID, err)
	}
	now := time.Now().UTC()

	affected, err := s.queries.UpdateExperiment(ctx, postgresdb.UpdateExperimentParams{
		ID: experiment.ID, Name: experiment.Name, Variants: variants, Enabled: experiment.Enabled,
		UpdatedAt: pgTimestamptz(now),
	})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: update experiment %s: %w", experiment.ID, errors.Join(err, ErrBackendUnavailable))
	}
	if affected == 0 {
		return ErrExperimentNotFound
	}
	experiment.UpdatedAt = now
	return nil
}

func (s *postgresExperimentStore) DeleteExperiment(ctx context.Context, experimentID string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := s.queries.DeleteExperiment(ctx, experimentID); err != nil {
		return fmt.Errorf("grnoti/postgres: delete experiment %s: %w", experimentID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (s *postgresExperimentStore) ListExperiments(ctx context.Context) ([]*Experiment, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := s.queries.ListExperiments(ctx)
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: list experiments: %w", errors.Join(err, ErrBackendUnavailable))
	}
	out := make([]*Experiment, 0, len(rows))
	for _, row := range rows {
		e, err := experimentRowToDomain(row)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func (s *postgresExperimentStore) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.ownsPool {
			s.pool.Close()
		}
		s.logger.Info("grnoti/postgres: experiment store closed")
	})
	return nil
}
