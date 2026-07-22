// File: preferences.postgres.go

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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/gourdian25/grnoti/internal/postgresdb"
)

func preferencesRowToDomain(r postgresdb.GrnotiPreference) (*NotificationPreferences, error) {
	settings := make(map[EventType]bool)
	if len(r.EventTypeSettings) > 0 {
		if err := json.Unmarshal(r.EventTypeSettings, &settings); err != nil {
			return nil, fmt.Errorf("grnoti/postgres: decode event type settings for %s: %w", r.UserID, err)
		}
	}
	return &NotificationPreferences{
		UserID: r.UserID, GlobalEnabled: r.GlobalEnabled, QuietHoursEnabled: r.QuietHoursEnabled,
		QuietHoursStart: r.QuietHoursStart, QuietHoursEnd: r.QuietHoursEnd, Timezone: r.Timezone, Locale: r.Locale,
		EventTypeSettings: settings, CreatedAt: pgTime(r.CreatedAt), UpdatedAt: pgTime(r.UpdatedAt),
	}, nil
}

type postgresPreferencesStore struct {
	pool     *pgxpool.Pool
	queries  *postgresdb.Queries
	logger   Logger
	ownsPool bool

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ PreferencesStore = (*postgresPreferencesStore)(nil)

// NewPostgresPreferencesStore connects per cfg — the source-of-truth
// PreferencesStore backend (see docs/plan/grnoti-plan.md §6; pair with
// NewCachedPreferencesStore for the Redis read-through cache in front of
// it).
func NewPostgresPreferencesStore(cfg PostgresConfig) (PreferencesStore, error) {
	pool, queries, ownsPool, err := connectPostgres(context.Background(), cfg, "PreferencesStore")
	if err != nil {
		return nil, err
	}
	logger := OrNop(cfg.Logger)
	logger.Info("grnoti/postgres: preferences store connected")
	return &postgresPreferencesStore{pool: pool, queries: queries, logger: logger, ownsPool: ownsPool}, nil
}

func (s *postgresPreferencesStore) GetPreferences(ctx context.Context, userID string) (*NotificationPreferences, error) {
	if s.closed.Load() {
		return nil, ErrClosed
	}
	row, err := s.queries.GetPreferences(ctx, userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrPreferencesNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("grnoti/postgres: get preferences for %s: %w", userID, errors.Join(err, ErrBackendUnavailable))
	}
	return preferencesRowToDomain(row)
}

func (s *postgresPreferencesStore) SavePreferences(ctx context.Context, prefs *NotificationPreferences) error {
	if s.closed.Load() {
		return ErrClosed
	}
	if prefs.UserID == "" {
		return ErrPreferencesUserIDRequired
	}
	settings, err := json.Marshal(prefs.EventTypeSettings)
	if err != nil {
		return fmt.Errorf("grnoti/postgres: encode event type settings for %s: %w", prefs.UserID, err)
	}

	err = s.queries.UpsertPreferences(ctx, postgresdb.UpsertPreferencesParams{
		UserID: prefs.UserID, GlobalEnabled: prefs.GlobalEnabled, QuietHoursEnabled: prefs.QuietHoursEnabled,
		QuietHoursStart: prefs.QuietHoursStart, QuietHoursEnd: prefs.QuietHoursEnd,
		Timezone: prefs.Timezone, Locale: prefs.Locale, EventTypeSettings: settings,
		CreatedAt: pgTimestamptz(time.Now().UTC()),
	})
	if err != nil {
		return fmt.Errorf("grnoti/postgres: save preferences for %s: %w", prefs.UserID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

func (s *postgresPreferencesStore) IsEventTypeEnabled(ctx context.Context, userID string, eventType EventType) (bool, error) {
	prefs, err := s.GetPreferences(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrPreferencesNotFound) {
			return true, nil
		}
		return false, err
	}
	return prefs.IsEventTypeEnabled(eventType), nil
}

func (s *postgresPreferencesStore) Close() error {
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.ownsPool {
			s.pool.Close()
		}
		s.logger.Info("grnoti/postgres: preferences store closed")
	})
	return nil
}
