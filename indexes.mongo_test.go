// File: indexes.mongo_test.go

package grnoti

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// SkipIndexCreation (v0.6.0): a deployment whose migrations own the
// collections, and whose application user may not create indexes, has the
// stores create nothing and check the indexes they rely on instead.

// skipIndexCase builds one Mongo store on collection with SkipIndexCreation
// set or not, and closes it.
type skipIndexCase struct {
	name    string
	indexes []mongoIndex
	build   func(collection string, skip bool) error
}

func skipIndexCases() []skipIndexCase {
	return []skipIndexCase{
		{"devices", deviceIndexes, func(c string, skip bool) error {
			s, err := NewMongoDeviceStore(MongoDeviceStoreConfig{URI: testMongoURI, Database: "grnoti_test", CollectionName: c, SkipIndexCreation: skip})
			if err == nil {
				_ = s.Close()
			}
			return err
		}},
		{"preferences", preferencesIndexes, func(c string, skip bool) error {
			s, err := NewMongoPreferencesStore(MongoPreferencesStoreConfig{URI: testMongoURI, Database: "grnoti_test", CollectionName: c, SkipIndexCreation: skip})
			if err == nil {
				_ = s.Close()
			}
			return err
		}},
		{"dlq", dlqIndexes, func(c string, skip bool) error {
			s, err := NewMongoDLQHandler(MongoDLQHandlerConfig{URI: testMongoURI, Database: "grnoti_test", CollectionName: c, SkipIndexCreation: skip})
			if err == nil {
				_ = s.Close()
			}
			return err
		}},
	}
}

func testMongoDB(t *testing.T) *mongo.Database {
	t.Helper()
	client, err := mongo.Connect(options.Client().ApplyURI(testMongoURI))
	if err != nil {
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		t.Skipf("MongoDB not available at %s, skipping: %v", testMongoURI, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	return client.Database("grnoti_test")
}

func TestSkipIndexCreation_ChecksInsteadOfCreating(t *testing.T) {
	db := testMongoDB(t)
	ctx := context.Background()
	for _, c := range skipIndexCases() {
		t.Run(c.name, func(t *testing.T) {
			// No collection at all: every index is missing, and nothing is made.
			empty := fmt.Sprintf("skip_%s_empty_%d", c.name, time.Now().UnixNano())
			if err := c.build(empty, true); !errors.Is(err, ErrMongoIndexMissing) {
				t.Fatalf("skip on a collection without its indexes = %v, want ErrMongoIndexMissing", err)
			}
			if n := countIndexes(t, db.Collection(empty)); n != 0 {
				t.Fatalf("skip created %d indexes, want none", n)
			}

			// What the default constructor makes is exactly what skip accepts,
			// whatever the indexes are named.
			made := fmt.Sprintf("skip_%s_made_%d", c.name, time.Now().UnixNano())
			if err := c.build(made, false); err != nil {
				t.Fatalf("default constructor: %v", err)
			}
			if err := c.build(made, true); err != nil {
				t.Fatalf("skip on the indexes the default made: %v", err)
			}
			named := fmt.Sprintf("skip_%s_named_%d", c.name, time.Now().UnixNano())
			models := make([]mongo.IndexModel, 0, len(c.indexes))
			for i, ix := range c.indexes {
				m := ix.model()
				m.Options.SetName(fmt.Sprintf("migration_named_%d", i))
				models = append(models, m)
			}
			if _, err := db.Collection(named).Indexes().CreateMany(ctx, models); err != nil {
				t.Fatalf("create the indexes by other names: %v", err)
			}
			if err := c.build(named, true); err != nil {
				t.Fatalf("skip on the same indexes named otherwise: %v", err)
			}

			// Each index, missing or not quite the same, is refused.
			for i := range c.indexes {
				for _, variant := range []struct {
					what string
					ix   *mongoIndex
				}{
					{"missing", nil},
					{"without its uniqueness or TTL, or with keys reversed", weakened(c.indexes[i])},
				} {
					coll := fmt.Sprintf("skip_%s_%d_%d", c.name, i, time.Now().UnixNano())
					var ms []mongo.IndexModel
					for j, ix := range c.indexes {
						if j != i {
							ms = append(ms, ix.model())
						} else if variant.ix != nil {
							ms = append(ms, variant.ix.model())
						}
					}
					if len(ms) > 0 {
						if _, err := db.Collection(coll).Indexes().CreateMany(ctx, ms); err != nil {
							t.Fatalf("create: %v", err)
						}
					}
					err := c.build(coll, true)
					if !errors.Is(err, ErrMongoIndexMissing) {
						t.Fatalf("index %s %s: %v, want ErrMongoIndexMissing", c.indexes[i], variant.what, err)
					}
				}
			}
		})
	}
}

// weakened is ix with what makes it matter taken away: its uniqueness, its
// TTL, or (for a plain compound index) its keys' order.
func weakened(ix mongoIndex) *mongoIndex {
	w := ix
	switch {
	case ix.unique:
		w.unique = false
	case ix.ttlSecond > 0:
		w.ttlSecond = ix.ttlSecond + 1
	default:
		w.keys = bson.D{ix.keys[1], ix.keys[0]}
	}
	return &w
}

func countIndexes(t *testing.T, coll *mongo.Collection) int {
	t.Helper()
	cursor, err := coll.Indexes().List(context.Background())
	if err != nil {
		var ce mongo.CommandError
		if errors.As(err, &ce) && ce.Code == 26 { // NamespaceNotFound: no collection, so no index
			return 0
		}
		t.Fatalf("list indexes: %v", err)
	}
	var all []bson.M
	if err := cursor.All(context.Background(), &all); err != nil {
		t.Fatalf("read indexes: %v", err)
	}
	return len(all)
}

func TestSameKeysAndIndexString(t *testing.T) {
	a := bson.D{{Key: "user_id", Value: 1}, {Key: "is_active", Value: 1}}
	for name, c := range map[string]struct {
		have bson.D
		want bool
	}{
		"same, int32":      {bson.D{{Key: "user_id", Value: int32(1)}, {Key: "is_active", Value: int32(1)}}, true},
		"same, int64":      {bson.D{{Key: "user_id", Value: int64(1)}, {Key: "is_active", Value: int64(1)}}, true},
		"same, double":     {bson.D{{Key: "user_id", Value: 1.0}, {Key: "is_active", Value: 1.0}}, true},
		"reversed":         {bson.D{{Key: "is_active", Value: 1}, {Key: "user_id", Value: 1}}, false},
		"descending":       {bson.D{{Key: "user_id", Value: -1}, {Key: "is_active", Value: 1}}, false},
		"a prefix":         {bson.D{{Key: "user_id", Value: 1}}, false},
		"a text index":     {bson.D{{Key: "user_id", Value: "text"}, {Key: "is_active", Value: 1}}, false},
		"one field longer": {append(append(bson.D{}, a...), bson.E{Key: "x", Value: 1}), false},
	} {
		if got := sameKeys(c.have, a); got != c.want {
			t.Errorf("%s: sameKeys = %v, want %v", name, got, c.want)
		}
	}
	if got := dlqIndexes[2].String(); got != "{created_at: 1} ttl 604800s" {
		t.Errorf("String() = %q", got)
	}
	if got := deviceIndexes[0].String(); got != "{fid: 1} unique" {
		t.Errorf("String() = %q", got)
	}
	if got := deviceIndexes[1].String(); got != "{user_id: 1, is_active: 1}" {
		t.Errorf("String() = %q", got)
	}
}
