// File: indexes.mongo.go

package grnoti

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ErrMongoIndexMissing is a Mongo store's constructor refusing to start
// because SkipIndexCreation is set and an index the store relies on isn't
// there: the uniqueness a store relies on (one device per FID, one
// preferences document per user, one DLQ record per event) would otherwise
// go unenforced without a word.
var ErrMongoIndexMissing = errors.New("grnoti/mongo: a required index is missing")

// mongoIndex is an index a Mongo store needs: its keys in order, whether it
// is unique, and its TTL (0 for none).
type mongoIndex struct {
	keys      bson.D
	unique    bool
	ttlSecond int32
}

// ensureMongoIndexes either creates indexes on coll (the default), or, with
// skip set (a deployment whose migrations own the collection and whose
// application user may not create indexes), creates nothing and checks
// through listIndexes that each one exists with the same keys in the same
// order, the same uniqueness and the same TTL. A name is never compared:
// a migration may name an index as it likes.
func ensureMongoIndexes(ctx context.Context, coll *mongo.Collection, indexes []mongoIndex, skip bool) error {
	if !skip {
		models := make([]mongo.IndexModel, 0, len(indexes))
		for _, ix := range indexes {
			models = append(models, ix.model())
		}
		if _, err := coll.Indexes().CreateMany(ctx, models); err != nil {
			return fmt.Errorf("grnoti/mongo: ensure indexes: %w", err)
		}
		return nil
	}

	cursor, err := coll.Indexes().List(ctx)
	if err != nil {
		return fmt.Errorf("grnoti/mongo: list indexes: %w", errors.Join(err, ErrBackendUnavailable))
	}
	var existing []struct {
		Key                bson.D `bson:"key"`
		Unique             bool   `bson:"unique"`
		ExpireAfterSeconds *int32 `bson:"expireAfterSeconds"`
	}
	if err := cursor.All(ctx, &existing); err != nil {
		return fmt.Errorf("grnoti/mongo: read indexes: %w", errors.Join(err, ErrBackendUnavailable))
	}
	for _, want := range indexes {
		found := false
		for _, have := range existing {
			ttl := int32(0)
			if have.ExpireAfterSeconds != nil {
				ttl = *have.ExpireAfterSeconds
			}
			if sameKeys(have.Key, want.keys) && have.Unique == want.unique && ttl == want.ttlSecond {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("%w on %s: %s", ErrMongoIndexMissing, coll.Name(), want)
		}
	}
	return nil
}

// model is ix as the driver creates it.
func (ix mongoIndex) model() mongo.IndexModel {
	opts := options.Index()
	if ix.unique {
		opts.SetUnique(true)
	}
	if ix.ttlSecond > 0 {
		opts.SetExpireAfterSeconds(ix.ttlSecond)
	}
	return mongo.IndexModel{Keys: ix.keys, Options: opts}
}

// String names ix for an error: its keys, then unique and TTL when set.
func (ix mongoIndex) String() string {
	s := "{"
	for i, e := range ix.keys {
		if i > 0 {
			s += ", "
		}
		s += fmt.Sprintf("%s: %v", e.Key, e.Value)
	}
	s += "}"
	if ix.unique {
		s += " unique"
	}
	if ix.ttlSecond > 0 {
		s += fmt.Sprintf(" ttl %ds", ix.ttlSecond)
	}
	return s
}

// sameKeys reports whether two index key documents name the same fields in
// the same order with the same direction. The server may answer a direction
// as an int32, an int64 or a double, so each is compared as a number.
func sameKeys(have, want bson.D) bool {
	if len(have) != len(want) {
		return false
	}
	for i := range want {
		if have[i].Key != want[i].Key {
			return false
		}
		h, hok := number(have[i].Value)
		w, wok := number(want[i].Value)
		if !hok || !wok || h != w {
			return false
		}
	}
	return true
}

// number is v as a float64, when it is a number.
func number(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case float64:
		return n, true
	}
	return 0, false
}
