// File: contract_helpers_test.go

package grnoti

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// contractCollectionName derives a Mongo collection name from the
// currently-executing subtest, and cleanupContractCollection (registered
// alongside every call site) drops it at test end.
//
// t.Name() alone is NOT sufficient for isolation: it's stable *within* one
// `go test` invocation (avoiding collisions between subtests running in the
// same process) but identical *across* separate invocations, so without an
// explicit drop, a real MongoDB instance accumulates every prior run's
// documents under the same collection name — confirmed the hard way: a
// second test run failed on stale data left by the first (see
// docs/plan/grnoti-plan.md §11), the same class of bug as
// cleanupContractRows below, just surfacing in Mongo instead of Postgres.
func contractCollectionName(t *testing.T) string {
	t.Helper()
	name := "contract_" + t.Name()
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, " ", "_")
	return name
}

// cleanupContractCollection registers a t.Cleanup that drops the named
// Mongo collection, via its own short-lived connection decoupled from the
// store's lifecycle — same reasoning as cleanupContractRows: a defer in the
// test body closing the store always runs before t.Cleanup fires, so
// reusing the store's own connection here would silently no-op.
func cleanupContractCollection(t *testing.T, database, collection string) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(testMongoURI))
		if err != nil {
			return
		}
		defer client.Disconnect(ctx)
		if err := client.Database(database).Collection(collection).Drop(ctx); err != nil {
			t.Logf("cleanupContractCollection: %v", err)
		}
	})
}

// cleanupContractRows registers a t.Cleanup that deletes rows whose
// idColumn starts with "contract-" from table, scoped so it never touches
// data owned by the individual (non-contract) backend test files sharing
// the same live Postgres instance.
//
// Deliberately opens its own short-lived pool rather than taking the
// store's — a real bug found while writing these contract tests: the
// contract test bodies do `defer store.Close()`, and Go's `defer`
// statements inside a test function always run before that test's
// t.Cleanup-registered functions, regardless of registration order. A
// cleanup that reused the store's own (by-then-closed) pool silently did
// nothing, and `_, _ = pool.Exec(...)` discarded the resulting error — see
// docs/plan/grnoti-plan.md §11.
func cleanupContractRows(t *testing.T, table, idColumn string) {
	t.Helper()
	t.Cleanup(func() {
		pool, err := pgxpool.New(context.Background(), testPostgresDSN)
		if err != nil {
			return
		}
		defer pool.Close()
		if _, err := pool.Exec(context.Background(), "DELETE FROM "+table+" WHERE "+idColumn+" LIKE 'contract-%'"); err != nil {
			t.Logf("cleanupContractRows: %v", err)
		}
	})
}
