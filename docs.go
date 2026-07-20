// File: docs.go

// Package grnoti provides a push-notification service for the gourdian
// ecosystem: FCM dispatch, idempotent event processing, device-token
// management, durable dead-letter retry tracking, circuit breaking,
// distributed rate limiting, deterministic A/B experiment assignment,
// localization, and topic-based routing, behind a set of storage-agnostic
// interfaces.
//
// # Package shape
//
// grnoti's public API is a single flat package with no subpackages — every
// backend (MongoDB, PostgreSQL, Redis, Kafka, FCM) lives in this one
// module, distinguished by a "<concern>.<backend>.go" file-naming
// convention (e.g. tokenstore.mongo.go, dlq.postgres.go,
// ratelimiter.redis.go). The one exception is internal/postgresdb, sqlc's
// generated query code — it's a real Go subpackage, but an unexported
// internal/ one, not importable outside this module, so it doesn't
// undermine the "flat public API" claim. This is a deliberate divergence
// from sibling repos like grcache/graudit, which use one subpackage per
// backend to keep unused backend drivers out of a consumer's dependency
// graph — grnoti accepts that cost (importing grnoti pulls in the Mongo
// driver, pgx/v5 + sqlc-generated Postgres code, go-redis, sarama, and the
// Firebase messaging SDK regardless of which backends are actually used)
// in exchange for a simpler package structure, following gourdiantoken's
// precedent rather than grcache's. See docs/architecture.md for the full
// reasoning.
//
// # Precise, non-aspirational claims
//
// DLQHandler's durability guarantee is independent of grevents: an event
// dead-lettered here survives a process restart, unlike grevents'
// DeadLetterSink (an in-memory, best-effort recent-history buffer by
// design). grnoti optionally publishes lifecycle events
// ("notification.sent", "notification.failed", "experiment.assigned") via
// an injected grevents.Bus, but that publish is always best-effort — a nil
// bus or a publish failure never affects the durable operation it follows.
//
// ExperimentEngine's variant assignment is a pure, deterministic function
// of (userID, experiment.ID, experiment.Variants): the same inputs always
// produce the same variant, with or without the optional assignment cache.
package grnoti
