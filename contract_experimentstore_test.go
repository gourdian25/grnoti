// File: contract_experimentstore_test.go

package grnoti

import (
	"context"
	"testing"
)

// testExperimentStoreContract is the shared behavioral contract every
// ExperimentStore backend must satisfy identically. See
// testTokenStoreContract's doc comment for why newStore takes the
// currently-executing *testing.T.
func testExperimentStoreContract(t *testing.T, newStore func(t *testing.T) ExperimentStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("CreateThenGetRoundTrips", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		exp := &Experiment{ID: "contract-exp1", Name: "Test", Variants: []ExperimentVariant{{ID: "a", Weight: 1}}, Enabled: true}
		if err := store.CreateExperiment(ctx, exp); err != nil {
			t.Fatalf("CreateExperiment: %v", err)
		}
		got, err := store.GetExperiment(ctx, "contract-exp1")
		if err != nil {
			t.Fatalf("GetExperiment: %v", err)
		}
		if got.Name != "Test" || len(got.Variants) != 1 {
			t.Fatalf("GetExperiment() = %+v, want Name=Test with 1 variant", got)
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if _, err := store.GetExperiment(ctx, "contract-never-existed"); err != ErrExperimentNotFound {
			t.Fatalf("GetExperiment(missing) error = %v, want ErrExperimentNotFound", err)
		}
	})

	t.Run("UpdateNotFound", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		if err := store.UpdateExperiment(ctx, &Experiment{ID: "contract-never-existed-2"}); err != ErrExperimentNotFound {
			t.Fatalf("UpdateExperiment(missing) error = %v, want ErrExperimentNotFound", err)
		}
	})

	t.Run("UpdatePersists", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		exp := &Experiment{ID: "contract-exp2", Name: "Original", Variants: []ExperimentVariant{{ID: "a"}}}
		_ = store.CreateExperiment(ctx, exp)
		exp.Name = "Updated"
		if err := store.UpdateExperiment(ctx, exp); err != nil {
			t.Fatalf("UpdateExperiment: %v", err)
		}
		got, _ := store.GetExperiment(ctx, "contract-exp2")
		if got.Name != "Updated" {
			t.Fatalf("GetExperiment after update = %+v, want Name=Updated", got)
		}
	})

	t.Run("DeleteThenGetNotFound", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		exp := &Experiment{ID: "contract-exp3", Variants: []ExperimentVariant{{ID: "a"}}}
		_ = store.CreateExperiment(ctx, exp)
		if err := store.DeleteExperiment(ctx, "contract-exp3"); err != nil {
			t.Fatalf("DeleteExperiment: %v", err)
		}
		if _, err := store.GetExperiment(ctx, "contract-exp3"); err != ErrExperimentNotFound {
			t.Fatalf("GetExperiment(deleted) error = %v, want ErrExperimentNotFound", err)
		}
	})

	t.Run("ListIncludesCreated", func(t *testing.T) {
		store := newStore(t)
		defer store.Close()
		_ = store.CreateExperiment(ctx, &Experiment{ID: "contract-exp4", Variants: []ExperimentVariant{{ID: "a"}}})
		all, err := store.ListExperiments(ctx)
		if err != nil {
			t.Fatalf("ListExperiments: %v", err)
		}
		found := false
		for _, e := range all {
			if e.ID == "contract-exp4" {
				found = true
			}
		}
		if !found {
			t.Fatalf("ListExperiments() = %v, want it to include contract-exp4", all)
		}
	})
}

func TestExperimentStore_Contract(t *testing.T) {
	t.Run("Memory", func(t *testing.T) {
		testExperimentStoreContract(t, func(t *testing.T) ExperimentStore { return NewMemoryExperimentStore() })
	})
	t.Run("Postgres", func(t *testing.T) {
		testExperimentStoreContract(t, func(t *testing.T) ExperimentStore {
			store, err := NewPostgresExperimentStore(PostgresConfig{DSN: testPostgresDSN})
			if err != nil {
				t.Skipf("PostgreSQL not available, skipping: %v", err)
			}
			cleanupContractRows(t, "grnoti_experiments", "id")
			return store
		})
	})
}
