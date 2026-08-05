// File: experiment.go

package grnoti

import (
	"context"
	"crypto/md5" //nolint:gosec // used only for deterministic bucketing, not a security boundary
	"encoding/binary"
	"sync"

	"github.com/gourdian25/grevents"
)

// deterministicExperimentEngine computes variant assignment as a pure
// function of (userID, experiment.ID, experiment.Variants) and caches the
// result in a sync.RWMutex-protected map. Unlike the reference
// implementation's deterministicExperimentEngine — whose equivalent maps
// were mutated with zero synchronization (see docs/plan/grnoti-plan.md §2
// item 2) — every access here is lock-protected. AssignVariant holds a
// single write lock across its entire check-then-write sequence for a
// given key (see AssignVariant), so at most one goroutine can ever observe
// "not yet assigned" and proceed to compute+store+publish for that key —
// concurrent callers racing on a brand-new (userID, experimentID) pair get
// an exactly-once experiment.assigned publish (via PublishAssigned), not
// just a race-free map write. GetVariant still uses the cheaper RLock,
// since it only reads.
type deterministicExperimentEngine struct {
	mu          sync.RWMutex
	assignments map[string]ExperimentVariant // key: userID + ":" + experimentID

	// analytics is optional (nil-safe) — see TrackImpression/TrackConversion.
	analytics AnalyticsPublisher
	// bus is optional (nil-safe) — see AssignVariant/PublishAssigned.
	bus    grevents.Bus
	logger Logger
}

var _ ExperimentEngine = (*deterministicExperimentEngine)(nil)

// NewDeterministicExperimentEngine constructs an ExperimentEngine with an
// in-process assignment cache. See cache.experiment.go (Stage 4) for a
// grcache-backed variant suited to multi-instance deployments, where the
// in-process cache here would give each instance its own (still
// individually-correct, since assignment is deterministic) cache instead of
// a shared one.
//
// Parameters:
//   - analytics: AnalyticsPublisher — may be nil; TrackImpression/
//     TrackConversion log and no-op rather than erroring when unset
//   - bus: grevents.Bus — may be nil; AssignVariant publishes
//     TopicExperimentAssigned on a new assignment when set (§1.2)
//   - logger: Logger — may be nil
func NewDeterministicExperimentEngine(analytics AnalyticsPublisher, bus grevents.Bus, logger Logger) ExperimentEngine {
	return &deterministicExperimentEngine{
		assignments: make(map[string]ExperimentVariant),
		analytics:   analytics,
		bus:         bus,
		logger:      OrNop(logger),
	}
}

func assignmentKey(userID, experimentID string) string { return userID + ":" + experimentID }

func (e *deterministicExperimentEngine) GetVariant(_ context.Context, userID string, experimentID string) (*ExperimentVariant, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if v, ok := e.assignments[assignmentKey(userID, experimentID)]; ok {
		variant := v
		return &variant, nil
	}
	return nil, nil
}

func (e *deterministicExperimentEngine) AssignVariant(ctx context.Context, userID string, experiment *Experiment) (*ExperimentVariant, error) {
	if experiment == nil || len(experiment.Variants) == 0 {
		return nil, ErrExperimentHasNoVariants
	}
	key := assignmentKey(userID, experiment.ID)

	e.mu.Lock()
	if v, ok := e.assignments[key]; ok {
		e.mu.Unlock()
		variant := v
		return &variant, nil
	}
	variant := deterministicPick(userID, experiment)
	e.assignments[key] = variant
	e.mu.Unlock()

	PublishAssigned(ctx, e.bus, e.logger, ExperimentAssignedPayload{
		UserID: userID, ExperimentID: experiment.ID, VariantID: variant.ID,
	})

	return &variant, nil
}

// deterministicPick hashes userID+experiment.ID into a bucket in
// [0, totalWeight) and returns whichever variant's cumulative weight range
// contains that bucket. A variant with Weight<=0 is treated as weight 1.
func deterministicPick(userID string, experiment *Experiment) ExperimentVariant {
	totalWeight := 0
	for _, v := range experiment.Variants {
		w := v.Weight
		if w <= 0 {
			w = 1
		}
		totalWeight += w
	}

	h := md5.Sum([]byte(userID + ":" + experiment.ID))                  //nolint:gosec // deterministic bucketing only
	bucket := int(binary.BigEndian.Uint32(h[:4]) % uint32(totalWeight)) //nolint:gosec // totalWeight > 0, guaranteed by the empty-Variants check in AssignVariant

	cumulative := 0
	for _, v := range experiment.Variants {
		w := v.Weight
		if w <= 0 {
			w = 1
		}
		cumulative += w
		if bucket < cumulative {
			return v
		}
	}
	return experiment.Variants[len(experiment.Variants)-1] // unreachable if totalWeight computed correctly above
}

// TrackImpression publishes a real analytics event via the configured
// AnalyticsPublisher. Unlike the reference implementation's hardcoded
// no-op (see docs/plan/grnoti-plan.md §2 item 9), a missing
// AnalyticsPublisher is visibly logged, not silently dropped.
func (e *deterministicExperimentEngine) TrackImpression(ctx context.Context, userID string, experimentID string, variantID string) error {
	if e.analytics == nil {
		e.logger.Warn("grnoti: TrackImpression dropped, no AnalyticsPublisher configured", "user_id", userID, "experiment_id", experimentID, "variant_id", variantID)
		return nil
	}
	return e.analytics.PublishImpression(ctx, userID, experimentID, variantID)
}

// TrackConversion publishes a real analytics event via the configured
// AnalyticsPublisher. See TrackImpression.
func (e *deterministicExperimentEngine) TrackConversion(ctx context.Context, userID string, experimentID string) error {
	if e.analytics == nil {
		e.logger.Warn("grnoti: TrackConversion dropped, no AnalyticsPublisher configured", "user_id", userID, "experiment_id", experimentID)
		return nil
	}
	return e.analytics.PublishConversion(ctx, userID, experimentID)
}
