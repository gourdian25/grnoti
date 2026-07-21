// File: memory.go

package grnoti

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// memoryTokenStore is an in-memory TokenStore for tests and single-instance
// development — not for production use (no persistence across restarts).
type memoryTokenStore struct {
	mu     sync.RWMutex
	tokens map[string]DeviceToken // key: token string
}

var _ TokenStore = (*memoryTokenStore)(nil)

// NewMemoryTokenStore constructs an in-memory TokenStore.
func NewMemoryTokenStore() TokenStore {
	return &memoryTokenStore{tokens: make(map[string]DeviceToken)}
}

func (s *memoryTokenStore) GetActiveTokens(_ context.Context, userID string) ([]DeviceToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []DeviceToken
	for _, t := range s.tokens {
		if t.UserID == userID && t.IsActive {
			out = append(out, t)
		}
	}
	return out, nil
}

func (s *memoryTokenStore) GetActiveTokensBatch(_ context.Context, userIDs []string) (map[string][]DeviceToken, error) {
	want := make(map[string]struct{}, len(userIDs))
	for _, id := range userIDs {
		want[id] = struct{}{}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]DeviceToken)
	for _, t := range s.tokens {
		if !t.IsActive {
			continue
		}
		if _, ok := want[t.UserID]; ok {
			out[t.UserID] = append(out[t.UserID], t)
		}
	}
	return out, nil
}

func (s *memoryTokenStore) GetActiveTokensByAnonymousID(_ context.Context, anonymousID string) ([]DeviceToken, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []DeviceToken
	for _, t := range s.tokens {
		if t.AnonymousID == anonymousID && t.IsActive {
			out = append(out, t)
		}
	}
	return out, nil
}

func (s *memoryTokenStore) MarkInvalid(_ context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tokens[token]; ok {
		t.IsActive = false
		t.UpdatedAt = time.Now().UTC()
		s.tokens[token] = t
	}
	return nil
}

func (s *memoryTokenStore) SaveToken(_ context.Context, token DeviceToken) error {
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.tokens[token.Token]; ok {
		token.CreatedAt = existing.CreatedAt
	} else {
		token.CreatedAt = now
	}
	token.IsActive = true
	token.UpdatedAt = now
	s.tokens[token.Token] = token
	return nil
}

func (s *memoryTokenStore) DeleteToken(_ context.Context, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tokens, token)
	return nil
}

func (s *memoryTokenStore) Close() error { return nil }

// memoryPreferencesStore is an in-memory PreferencesStore. Unlike the
// reference implementation's InMemoryPreferencesStore — whose map was
// mutated with zero synchronization (see docs/plan/grnoti-plan.md §2 item
// 3) — every access here is protected by a real sync.RWMutex.
type memoryPreferencesStore struct {
	mu    sync.RWMutex
	byUID map[string]*NotificationPreferences
}

var _ PreferencesStore = (*memoryPreferencesStore)(nil)

// NewMemoryPreferencesStore constructs an in-memory PreferencesStore.
func NewMemoryPreferencesStore() PreferencesStore {
	return &memoryPreferencesStore{byUID: make(map[string]*NotificationPreferences)}
}

func (s *memoryPreferencesStore) GetPreferences(_ context.Context, userID string) (*NotificationPreferences, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	prefs, ok := s.byUID[userID]
	if !ok {
		return nil, ErrPreferencesNotFound
	}
	copied := *prefs
	return &copied, nil
}

func (s *memoryPreferencesStore) SavePreferences(_ context.Context, prefs *NotificationPreferences) error {
	if prefs.UserID == "" {
		return ErrPreferencesUserIDRequired
	}
	now := time.Now().UTC()
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := *prefs
	if existing, ok := s.byUID[prefs.UserID]; ok {
		copied.CreatedAt = existing.CreatedAt
	} else {
		copied.CreatedAt = now
	}
	copied.UpdatedAt = now
	s.byUID[prefs.UserID] = &copied
	return nil
}

func (s *memoryPreferencesStore) IsEventTypeEnabled(ctx context.Context, userID string, eventType EventType) (bool, error) {
	prefs, err := s.GetPreferences(ctx, userID)
	if err != nil {
		if errors.Is(err, ErrPreferencesNotFound) {
			return true, nil // unconfigured user is opted in, not opted out
		}
		return false, err
	}
	return prefs.IsEventTypeEnabled(eventType), nil
}

func (s *memoryPreferencesStore) Close() error { return nil }

// memoryDLQHandler is an in-memory DLQHandler. Its ClaimRetryableEvents
// implements the same atomic-claim contract every backend must (see
// docs/plan/grnoti-plan.md §5): the claim scan and the pending->retrying
// status transition happen under one mutex hold, so two concurrent callers
// on the same instance can never both claim the same event.
type memoryDLQHandler struct {
	mu     sync.Mutex
	events map[string]*DLQEvent
	config dlqBackoffConfig
}

type dlqBackoffConfig struct {
	maxRetries    int
	retryDelay    time.Duration
	maxRetryDelay time.Duration
}

var _ DLQHandler = (*memoryDLQHandler)(nil)

// NewMemoryDLQHandler constructs an in-memory DLQHandler.
//
// Parameters:
//   - maxRetries: int — defaults to 3 if <= 0
//   - retryDelay, maxRetryDelay: time.Duration — passed to
//     FullJitterBackoff for computing each event's NextRetryAt; unlike
//     maxRetries, 0 is a valid, deliberate choice here (immediate
//     retry-eligibility, useful for tests), not silently replaced with a
//     default — pass 5*time.Minute/time.Hour explicitly for the values the
//     Postgres/Mongo backends use as their own defaults
func NewMemoryDLQHandler(maxRetries int, retryDelay, maxRetryDelay time.Duration) DLQHandler {
	if maxRetries <= 0 {
		maxRetries = 3
	}
	return &memoryDLQHandler{
		events: make(map[string]*DLQEvent),
		config: dlqBackoffConfig{maxRetries: maxRetries, retryDelay: retryDelay, maxRetryDelay: maxRetryDelay},
	}
}

func (h *memoryDLQHandler) PublishToDLQ(_ context.Context, event Event, failureReason string) error {
	now := time.Now().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()

	if existing, ok := h.events[event.EventID]; ok {
		existing.FailureReason = failureReason
		existing.LastAttemptAt = now
		existing.UpdatedAt = now
		existing.AttemptHistory = append(existing.AttemptHistory, DLQRetryAttempt{
			AttemptNumber: existing.RetryCount,
			AttemptedAt:   now,
			Success:       false,
			ErrorMessage:  failureReason,
		})
		return nil
	}

	h.events[event.EventID] = &DLQEvent{
		EventID:        event.EventID,
		Event:          event,
		FailureReason:  failureReason,
		MaxRetries:     h.config.maxRetries,
		FirstFailureAt: now,
		LastAttemptAt:  now,
		NextRetryAt:    now.Add(h.config.retryDelay),
		Status:         DLQStatusPending,
		AttemptHistory: []DLQRetryAttempt{{AttemptNumber: 0, AttemptedAt: now, Success: false, ErrorMessage: failureReason}},
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	return nil
}

func (h *memoryDLQHandler) ClaimRetryableEvents(_ context.Context, limit int) ([]*DLQEvent, error) {
	now := time.Now().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()

	var candidates []*DLQEvent
	for _, e := range h.events {
		if e.Status == DLQStatusPending && !e.NextRetryAt.After(now) {
			candidates = append(candidates, e)
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].NextRetryAt.Before(candidates[j].NextRetryAt) })

	if limit > 0 && len(candidates) > limit {
		candidates = candidates[:limit]
	}

	claimed := make([]*DLQEvent, 0, len(candidates))
	for _, e := range candidates {
		e.Status = DLQStatusRetrying
		e.UpdatedAt = now
		copied := *e
		claimed = append(claimed, &copied)
	}
	return claimed, nil
}

func (h *memoryDLQHandler) MarkRetried(_ context.Context, eventID string, success bool, attemptErr error) error {
	now := time.Now().UTC()
	h.mu.Lock()
	defer h.mu.Unlock()

	e, ok := h.events[eventID]
	if !ok {
		return ErrDLQEventNotFound
	}
	if e.Status != DLQStatusRetrying {
		return ErrDLQEventNotClaimed
	}

	newRetryCount := e.RetryCount + 1
	errMsg := ""
	if attemptErr != nil {
		errMsg = attemptErr.Error()
	}

	switch {
	case success:
		e.Status = DLQStatusResolved
	case newRetryCount >= e.MaxRetries:
		e.Status = DLQStatusExhausted
	default:
		e.Status = DLQStatusPending
		e.NextRetryAt = now.Add(FullJitterBackoff(h.config.retryDelay, h.config.maxRetryDelay, newRetryCount))
	}

	e.RetryCount = newRetryCount
	e.LastAttemptAt = now
	e.UpdatedAt = now
	e.AttemptHistory = append(e.AttemptHistory, DLQRetryAttempt{
		AttemptNumber: newRetryCount,
		AttemptedAt:   now,
		Success:       success,
		ErrorMessage:  errMsg,
	})
	return nil
}

func (h *memoryDLQHandler) GetEventByID(_ context.Context, eventID string) (*DLQEvent, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.events[eventID]
	if !ok {
		return nil, ErrDLQEventNotFound
	}
	copied := *e
	return &copied, nil
}

func (h *memoryDLQHandler) PurgeExpiredEvents(_ context.Context, maxAge time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-maxAge)
	h.mu.Lock()
	defer h.mu.Unlock()
	var purged int64
	for id, e := range h.events {
		if e.Status == DLQStatusResolved || e.Status == DLQStatusExhausted || e.CreatedAt.Before(cutoff) {
			delete(h.events, id)
			purged++
		}
	}
	return purged, nil
}

func (h *memoryDLQHandler) Close() error { return nil }

// memoryExperimentStore is an in-memory ExperimentStore.
type memoryExperimentStore struct {
	mu          sync.RWMutex
	experiments map[string]*Experiment
}

var _ ExperimentStore = (*memoryExperimentStore)(nil)

// NewMemoryExperimentStore constructs an in-memory ExperimentStore.
func NewMemoryExperimentStore() ExperimentStore {
	return &memoryExperimentStore{experiments: make(map[string]*Experiment)}
}

func (s *memoryExperimentStore) CreateExperiment(_ context.Context, experiment *Experiment) error {
	now := time.Now().UTC()
	experiment.CreatedAt = now
	experiment.UpdatedAt = now
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := *experiment
	s.experiments[experiment.ID] = &copied
	return nil
}

func (s *memoryExperimentStore) GetExperiment(_ context.Context, experimentID string) (*Experiment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.experiments[experimentID]
	if !ok {
		return nil, ErrExperimentNotFound
	}
	copied := *e
	return &copied, nil
}

func (s *memoryExperimentStore) UpdateExperiment(_ context.Context, experiment *Experiment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.experiments[experiment.ID]
	if !ok {
		return ErrExperimentNotFound
	}
	experiment.CreatedAt = existing.CreatedAt
	experiment.UpdatedAt = time.Now().UTC()
	copied := *experiment
	s.experiments[experiment.ID] = &copied
	return nil
}

func (s *memoryExperimentStore) DeleteExperiment(_ context.Context, experimentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.experiments, experimentID)
	return nil
}

func (s *memoryExperimentStore) ListExperiments(_ context.Context) ([]*Experiment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Experiment, 0, len(s.experiments))
	for _, e := range s.experiments {
		copied := *e
		out = append(out, &copied)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *memoryExperimentStore) Close() error { return nil }
