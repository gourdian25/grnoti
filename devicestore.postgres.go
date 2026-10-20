// File: tokenstore.postgres.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gourdian25/grnoti/internal/postgresdb"
)

func tokenRowToDomain(r postgresdb.GrnotiToken) DeviceToken {
	return DeviceToken{
		Token: r.Token, Platform: Platform(r.Platform), UserID: r.UserID, AnonymousID: r.AnonymousID,
		DeviceID: r.DeviceID, AppVersion: r.AppVersion, IsActive: r.IsActive,
		CreatedAt: pgTime(r.CreatedAt), UpdatedAt: pgTime(r.UpdatedAt),
	}
}

type postgresTokenStore struct {
	pool     *pgxpool.Pool
	queries  *postgresdb.Queries
	logger   Logger
	ownsPool bool

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ TokenStore = (*postgresTokenStore)(nil)

// NewPostgresTokenStore connects per cfg (pgx + sqlc-generated queries) —
// the alternative to the primary MongoDB backend (tokenstore.mongo.go);
// see docs/plan/grnoti-plan.md §6.
func NewPostgresTokenStore(cfg PostgresConfig) (TokenStore, error) {
	pool, queries, ownsPool, err := connectPostgres(context.Background(), cfg, "TokenStore")
	if err != nil {
		return nil, err
	}
	logger := OrNop(cfg.Logger)
	logger.Info("grnoti/postgres: token store connected")
	return &postgresTokenStore{pool: pool, queries: queries, logger: logger, ownsPool: ownsPool}, nil
}

func (s *postgresTokenStore) GetActiveTokens(ctx context.Context, userID string) ([]DeviceToken, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := s.queries.GetActiveTokensByUserID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get active tokens for %s: %w", userID, errors.Join(err, ErrBackendUnavailable))
	}
	out := make([]DeviceToken, len(rows))
	for i, r := range rows {
		out[i] = tokenRowToDomain(r)
	}
	return out, nil
}

func (s *postgresTokenStore) GetActiveTokensBatch(ctx context.Context, userIDs []string) (map[string][]DeviceToken, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := s.queries.GetActiveTokensByUserIDs(ctx, userIDs)
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get active tokens batch: %w", errors.Join(err, ErrBackendUnavailable))
	}
	out := make(map[string][]DeviceToken)
	for _, r := range rows {
		out[r.UserID] = append(out[r.UserID], tokenRowToDomain(r))
	}
	return out, nil
}

func (s *postgresTokenStore) GetActiveTokensByAnonymousID(ctx context.Context, anonymousID string) ([]DeviceToken, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := s.queries.GetActiveTokensByAnonymousID(ctx, anonymousID)
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get active tokens for anonymous %s: %w", anonymousID, errors.Join(err, ErrBackendUnavailable))
	}
	out := make([]DeviceToken, len(rows))
	for i, r := range rows {
		out[i] = tokenRowToDomain(r)
	}
	return out, nil
}

func (s *postgresTokenStore) MarkInvalid(ctx context.Context, token string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	affected, err := s.queries.MarkTokenInvalid(ctx, postgresdb.MarkTokenInvalidParams{
		Token: token, UpdatedAt: pgTimestamptz(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: mark invalid %s: %w", token, errors.Join(err, ErrBackendUnavailable))
	}
	if affected == 0 {
		s.logger.Debug("grnoti/postgres: MarkInvalid: token not found", "token", token)
	}
	return nil
}

func (s *postgresTokenStore) SaveToken(ctx context.Context, token DeviceToken) error {
	if s.closed.Load() {
		return ErrClosed
	}
	err := s.queries.UpsertToken(ctx, postgresdb.UpsertTokenParams{
		Token: token.Token, Platform: string(token.Platform), UserID: token.UserID, AnonymousID: token.AnonymousID,
		DeviceID: token.DeviceID, AppVersion: token.AppVersion, CreatedAt: pgTimestamptz(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: save token: %w", errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (s *postgresTokenStore) DeleteToken(ctx context.Context, token string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	affected, err := s.queries.DeleteToken(ctx, token)
	if err != nil {
		return fmt.Errorf("grnoti/postgres: delete token %s: %w", token, errors.Join(err, ErrBackendUnavailable))
	}
	if affected == 0 {
		s.logger.Debug("grnoti/postgres: DeleteToken: token not found", "token", token)
	}
	return nil
}

func (s *postgresTokenStore) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.ownsPool {
			s.pool.Close()
		}
		s.logger.Info("grnoti/postgres: token store closed")
	})
	return nil
}
