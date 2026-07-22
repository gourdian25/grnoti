// File: cache.experiment.go

package grnoti

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/gourdian25/grcache"
	"github.com/gourdian25/grevents"
)

// cacheExperimentEngine is ExperimentEngine backed by a grcache.Cache for
// its assignment cache instead of deterministicExperimentEngine's
// in-process map — useful for multi-instance deployments where sharing
// the assignment cache (e.g. via grcache/redis) avoids each instance
// recomputing/re-caching independently. This is purely a performance
// choice, not a correctness one: assignment is deterministic, so every
// instance computes the identical variant for a given (userID,
// experiment) regardless of which cache (if any) is used — see
// docs/plan/grnoti-plan.md §1.1 and experiment.go's own doc comment.
type cacheExperimentEngine struct {
	cache     grcache.Cache
	analytics AnalyticsPublisher
	// bus is optional (nil-safe) — see AssignVariant/PublishAssigned.
	bus    grevents.Bus
	logger Logger
}

var _ ExperimentEngine = (*cacheExperimentEngine)(nil)

// NewCacheBackedExperimentEngine constructs an ExperimentEngine whose
// assignment cache is any grcache.Cache, for multi-instance deployments.
//
// Parameters:
//   - cache: grcache.Cache — caller-owned; not closed by this engine (it
//     has no Close method at all — see the ExperimentEngine interface)
//   - analytics: AnalyticsPublisher — may be nil, see TrackImpression
//   - bus: grevents.Bus — may be nil; AssignVariant publishes
//     TopicExperimentAssigned on a new assignment when set (§1.2)
//   - logger: Logger — may be nil
func NewCacheBackedExperimentEngine(cache grcache.Cache, analytics AnalyticsPublisher, bus grevents.Bus, logger Logger) ExperimentEngine {
	return &cacheExperimentEngine{cache: cache, analytics: analytics, bus: bus, logger: OrNop(logger)}
}

func (e *cacheExperimentEngine) GetVariant(ctx context.Context, userID string, experimentID string) (*ExperimentVariant, error) {
	raw, err := e.cache.Get(ctx, assignmentKey(userID, experimentID))
	if err != nil {
		if errors.Is(err, grcache.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, err
	}
	var variant ExperimentVariant
	if jsonErr := json.Unmarshal(raw, &variant); jsonErr != nil {
		e.logger.Warn("grnoti: corrupt cached assignment, treating as unassigned", "user_id", userID, "experiment_id", experimentID, "error", jsonErr)
		return nil, nil
	}
	return &variant, nil
}

func (e *cacheExperimentEngine) AssignVariant(ctx context.Context, userID string, experiment *Experiment) (*ExperimentVariant, error) {
	if experiment == nil || len(experiment.Variants) == 0 {
		return nil, ErrExperimentHasNoVariants
	}

	if existing, err := e.GetVariant(ctx, userID, experiment.ID); err == nil && existing != nil {
		return existing, nil
	}

	variant := deterministicPick(userID, experiment)

	if raw, jsonErr := json.Marshal(variant); jsonErr == nil {
		// 0 TTL: an assignment cache entry has no natural expiry — a
		// deterministic recomputation would produce the identical value
		// anyway, so there's no correctness reason to expire it, only a
		// memory-pressure one a caller can address via grcache's own
		// InvalidateTag/backend-level eviction if needed.
		if setErr := e.cache.Set(ctx, assignmentKey(userID, experiment.ID), raw, 0); setErr != nil {
			e.logger.Warn("grnoti: caching assignment failed", "user_id", userID, "experiment_id", experiment.ID, "error", setErr)
		}
	}

	PublishAssigned(ctx, e.bus, e.logger, ExperimentAssignedPayload{
		UserID: userID, ExperimentID: experiment.ID, VariantID: variant.ID,
	})

	return &variant, nil
}

func (e *cacheExperimentEngine) TrackImpression(ctx context.Context, userID string, experimentID string, variantID string) error {
	if e.analytics == nil {
		e.logger.Warn("grnoti: TrackImpression dropped, no AnalyticsPublisher configured", "user_id", userID, "experiment_id", experimentID, "variant_id", variantID)
		return nil
	}
	return e.analytics.PublishImpression(ctx, userID, experimentID, variantID)
}

func (e *cacheExperimentEngine) TrackConversion(ctx context.Context, userID string, experimentID string) error {
	if e.analytics == nil {
		e.logger.Warn("grnoti: TrackConversion dropped, no AnalyticsPublisher configured", "user_id", userID, "experiment_id", experimentID)
		return nil
	}
	return e.analytics.PublishConversion(ctx, userID, experimentID)
}
