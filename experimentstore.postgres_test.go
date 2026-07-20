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

func TestNewPostgresExperimentStore_ConnectError(t *testing.T) {
	if _, err := NewPostgresExperimentStore(PostgresConfig{}); err == nil {
		t.Fatal("NewPostgresExperimentStore(empty DSN) = nil error, want non-nil")
	}
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

// TestExperimentRowToDomain_MalformedVariants inserts a row whose variants
// column is valid JSON (JSONB guarantees that) but not the expected shape
// (an object, not a []ExperimentVariant-compatible array) — exercising
// experimentRowToDomain's json.Unmarshal error branch. See the analogous
// preferences.postgres_test.go comment for why a plain malformed-syntax
// insert can't reach this branch at all.
func TestExperimentRowToDomain_MalformedVariants(t *testing.T) {
	store := newTestPostgresExperimentStore(t)
	s, ok := store.(*postgresExperimentStore)
	if !ok {
		t.Fatal("store is not *postgresExperimentStore")
	}
	ctx := context.Background()
	_, err := s.pool.Exec(ctx, `INSERT INTO grnoti_experiments
		(id, name, variants, enabled, created_at, updated_at)
		VALUES ($1, 'bad', '{"not": "an array"}', true, now(), now())`, "pg-exp-malformed")
	if err != nil {
		t.Fatalf("raw insert: %v", err)
	}

	if _, err := store.GetExperiment(ctx, "pg-exp-malformed"); err == nil {
		t.Fatal("GetExperiment(malformed variants) = nil error, want non-nil")
	}
}

// TestPostgresExperimentStore_GenericQueryError uses an already-canceled
// context to force a real query-level error — see the analogous
// tokenstore.postgres_test.go comment for why this reaches a branch fault
// injection would otherwise require.
func TestPostgresExperimentStore_GenericQueryError(t *testing.T) {
	store := newTestPostgresExperimentStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := store.CreateExperiment(ctx, &Experiment{ID: "e1"}); err == nil {
		t.Error("CreateExperiment(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.GetExperiment(ctx, "e1"); err == nil {
		t.Error("GetExperiment(canceled ctx) = nil error, want non-nil")
	}
	if err := store.UpdateExperiment(ctx, &Experiment{ID: "e1"}); err == nil {
		t.Error("UpdateExperiment(canceled ctx) = nil error, want non-nil")
	}
	if err := store.DeleteExperiment(ctx, "e1"); err == nil {
		t.Error("DeleteExperiment(canceled ctx) = nil error, want non-nil")
	}
	if _, err := store.ListExperiments(ctx); err == nil {
		t.Error("ListExperiments(canceled ctx) = nil error, want non-nil")
	}
}

func TestPostgresExperimentStore_Close_Idempotent(t *testing.T) {
	store := newTestPostgresExperimentStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
}

func TestPostgresExperimentStore_AfterClose_EveryMethodReturnsErrClosed(t *testing.T) {
	store := newTestPostgresExperimentStore(t)
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ctx := context.Background()

	if err := store.CreateExperiment(ctx, &Experiment{ID: "e1"}); err != ErrClosed {
		t.Errorf("CreateExperiment after Close = %v, want ErrClosed", err)
	}
	if _, err := store.GetExperiment(ctx, "e1"); err != ErrClosed {
		t.Errorf("GetExperiment after Close = %v, want ErrClosed", err)
	}
	if err := store.UpdateExperiment(ctx, &Experiment{ID: "e1"}); err != ErrClosed {
		t.Errorf("UpdateExperiment after Close = %v, want ErrClosed", err)
	}
	if err := store.DeleteExperiment(ctx, "e1"); err != ErrClosed {
		t.Errorf("DeleteExperiment after Close = %v, want ErrClosed", err)
	}
	if _, err := store.ListExperiments(ctx); err != ErrClosed {
		t.Errorf("ListExperiments after Close = %v, want ErrClosed", err)
	}
}
