// File: devicestore.postgres.go

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

func deviceRowToDomain(r postgresdb.GrnotiDevice) Device {
	return Device{
		FID: r.Fid, Platform: Platform(r.Platform), UserID: r.UserID, AnonymousID: r.AnonymousID,
		DeviceID: r.DeviceID, AppVersion: r.AppVersion, IsActive: r.IsActive,
		CreatedAt: pgTime(r.CreatedAt), UpdatedAt: pgTime(r.UpdatedAt),
	}
}

type postgresDeviceStore struct {
	pool     *pgxpool.Pool
	queries  *postgresdb.Queries
	logger   Logger
	ownsPool bool

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ DeviceStore = (*postgresDeviceStore)(nil)

// NewPostgresDeviceStore connects per cfg (pgx + sqlc-generated queries) —
// the alternative to the primary MongoDB backend (devicestore.mongo.go);
// see docs/plan/grnoti-plan.md §6.
func NewPostgresDeviceStore(cfg PostgresConfig) (DeviceStore, error) {
	pool, queries, ownsPool, err := connectPostgres(context.Background(), cfg, "DeviceStore")
	if err != nil {
		return nil, err
	}
	logger := OrNop(cfg.Logger)
	logger.Info("grnoti/postgres: device store connected")
	return &postgresDeviceStore{pool: pool, queries: queries, logger: logger, ownsPool: ownsPool}, nil
}

func (s *postgresDeviceStore) GetActiveDevices(ctx context.Context, userID string) ([]Device, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := s.queries.GetActiveDevicesByUserID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get active devices for %s: %w", userID, errors.Join(err, ErrBackendUnavailable))
	}
	out := make([]Device, len(rows))
	for i, r := range rows {
		out[i] = deviceRowToDomain(r)
	}
	return out, nil
}

func (s *postgresDeviceStore) GetActiveDevicesBatch(ctx context.Context, userIDs []string) (map[string][]Device, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := s.queries.GetActiveDevicesByUserIDs(ctx, userIDs)
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get active devices batch: %w", errors.Join(err, ErrBackendUnavailable))
	}
	out := make(map[string][]Device)
	for _, r := range rows {
		out[r.UserID] = append(out[r.UserID], deviceRowToDomain(r))
	}
	return out, nil
}

func (s *postgresDeviceStore) GetActiveDevicesByAnonymousID(ctx context.Context, anonymousID string) ([]Device, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	rows, err := s.queries.GetActiveDevicesByAnonymousID(ctx, anonymousID)
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get active devices for anonymous %s: %w", anonymousID, errors.Join(err, ErrBackendUnavailable))
	}
	out := make([]Device, len(rows))
	for i, r := range rows {
		out[i] = deviceRowToDomain(r)
	}
	return out, nil
}

func (s *postgresDeviceStore) MarkInvalid(ctx context.Context, fid string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	affected, err := s.queries.MarkDeviceInvalid(ctx, postgresdb.MarkDeviceInvalidParams{
		Fid: fid, UpdatedAt: pgTimestamptz(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: mark invalid %s: %w", fid, errors.Join(err, ErrBackendUnavailable))
	}
	if affected == 0 {
		s.logger.Debug("grnoti/postgres: MarkInvalid: fid not found", "fid", maskFID(fid))
	}
	return nil
}

func (s *postgresDeviceStore) SaveDevice(ctx context.Context, device Device) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if err := device.Validate(); err != nil {
		return err
	}
	err := s.queries.UpsertDevice(ctx, postgresdb.UpsertDeviceParams{
		Fid: device.FID, Platform: string(device.Platform), UserID: device.UserID, AnonymousID: device.AnonymousID,
		DeviceID: device.DeviceID, AppVersion: device.AppVersion, CreatedAt: pgTimestamptz(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: save device: %w", errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (s *postgresDeviceStore) DeleteDevice(ctx context.Context, fid string) error {
	if s.closed.Load() {
		return ErrClosed
	}
	affected, err := s.queries.DeleteDevice(ctx, fid)
	if err != nil {
		return fmt.Errorf("grnoti/postgres: delete device %s: %w", fid, errors.Join(err, ErrBackendUnavailable))
	}
	if affected == 0 {
		s.logger.Debug("grnoti/postgres: DeleteDevice: fid not found", "fid", maskFID(fid))
	}
	return nil
}

func (s *postgresDeviceStore) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.ownsPool {
			s.pool.Close()
		}
		s.logger.Info("grnoti/postgres: device store closed")
	})
	return nil
}
