// File: experimentstore.postgres_test.go

package grnoti

import (
	"context"
	"testing"
)

func newTestPostgresExperimentStore(t *testing.T) ExperimentStore {
	t.Helper()
	store, err := NewPostgresExperimentStore(PostgresConfig{DSN: testPostgresDSN})
	if err != nil {
		t.Skipf("PostgreSQL not available, skipping: %v", err)
	}
	t.Cleanup(func() {
		if s, ok := store.(*postgresExperimentStore); ok {
			s.pool.Exec(context.Background(), "DELETE FROM grnoti_experiments")
		}
		_ = store.Close()
	})
	return store
}

func TestPostgresExperimentStore_CRUD(t *testing.T) {
	store := newTestPostgresExperimentStore(t)
	ctx := context.Background()

	exp := &Experiment{ID: "pg-exp-1", Name: "Test", Variants: []ExperimentVariant{{ID: "a", Weight: 1}}, Enabled: true}
	if err := store.CreateExperiment(ctx, exp); err != nil {
		t.Fatalf("CreateExperiment: %v", err)
	}

	got, err := store.GetExperiment(ctx, "pg-exp-1")
	if err != nil || got.Name != "Test" || len(got.Variants) != 1 {
		t.Fatalf("GetExperiment() = (%+v, %v), want Name=Test with 1 variant", got, err)
	}

	got.Name = "Updated"
	if err := store.UpdateExperiment(ctx, got); err != nil {
		t.Fatalf("UpdateExperiment: %v", err)
	}
	got2, _ := store.GetExperiment(ctx, "pg-exp-1")
	if got2.Name != "Updated" {
		t.Fatalf("GetExperiment after update = %+v, want Name=Updated", got2)
	}

	all, err := store.ListExperiments(ctx)
	if err != nil || len(all) != 1 {
		t.Fatalf("ListExperiments() = (%v, %v), want 1 entry", all, err)
	}

	if err := store.DeleteExperiment(ctx, "pg-exp-1"); err != nil {
		t.Fatalf("DeleteExperiment: %v", err)
	}
	if _, err := store.GetExperiment(ctx, "pg-exp-1"); err != ErrExperimentNotFound {
		t.Fatalf("GetExperiment(deleted) error = %v, want ErrExperimentNotFound", err)
	}
}

func TestPostgresExperimentStore_CreateDuplicate(t *testing.T) {
	store := newTestPostgresExperimentStore(t)
	ctx := context.Background()
	exp := &Experiment{ID: "pg-exp-dup", Variants: []ExperimentVariant{{ID: "a"}}}

	if err := store.CreateExperiment(ctx, exp); err != nil {
		t.Fatalf("CreateExperiment (first): %v", err)
	}
	if err := store.CreateExperiment(ctx, exp); err != ErrExperimentAlreadyExists {
		t.Fatalf("CreateExperiment (duplicate) error = %v, want ErrExperimentAlreadyExists", err)
	}
}

func TestPostgresExperimentStore_UpdateNotFound(t *testing.T) {
	store := newTestPostgresExperimentStore(t)
	err := store.UpdateExperiment(context.Background(), &Experiment{ID: "never-existed-pg"})
	if err != ErrExperimentNotFound {
		t.Fatalf("UpdateExperiment(nonexistent) error = %v, want ErrExperimentNotFound", err)
	}
}
