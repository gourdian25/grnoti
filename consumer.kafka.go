// File: consumer.kafka.go

package grnoti

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/IBM/sarama"
)

// KafkaConsumerConfig configures an EventConsumer constructed by
// NewKafkaEventConsumer.
type KafkaConsumerConfig struct {
	// Brokers is the Kafka bootstrap broker list. Required.
	Brokers []string
	// GroupID is the consumer group ID. Required.
	GroupID string
	// Topics are the topics to subscribe to. Required, non-empty.
	Topics []string
	// SaramaConfig, if nil, defaults to a config matching the reference
	// implementation's own defaults (sarama.V3_0_0_0, round-robin
	// rebalancing, OffsetOldest, Consumer.Return.Errors=true).
	SaramaConfig *sarama.Config
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

// kafkaEventConsumer implements EventConsumer using Kafka via
// github.com/IBM/sarama's consumer-group API. Structurally the same shape
// as the reference implementation (JSON-decode each message into an Event,
// mark-then-skip on decode failure, mark-only-on-handler-success for
// at-least-once redelivery of processing failures) — the one defect fix in
// scope for this file is using grnoti's own Logger interface instead of a
// hardcoded *grlog.Logger (defect #5, docs/plan/grnoti-plan.md §2).
//
// The consumer itself has no compile-time dependency on WorkerPool: Start's
// handler parameter is supplied by the caller, so "wiring to WorkerPool"
// (§3.1) means a caller passes a handler that does pool.Submit(event) —
// proven end-to-end by TestKafkaEventConsumer_WiresToWorkerPool — rather
// than this file importing WorkerPool directly. The full ingestion→
// processing composition lives in service.go (Stage 12).
type kafkaEventConsumer struct {
	consumerGroup sarama.ConsumerGroup
	topics        []string
	logger        Logger

	mu      sync.Mutex
	handler func(context.Context, Event) error

	ready     chan struct{}
	readyOnce sync.Once

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ EventConsumer = (*kafkaEventConsumer)(nil)
var _ sarama.ConsumerGroupHandler = (*kafkaEventConsumer)(nil)

// NewKafkaEventConsumer creates a Kafka-backed EventConsumer, validating
// broker connectivity by constructing the underlying consumer group (which
// sarama connects eagerly, unlike a lazy client).
func NewKafkaEventConsumer(cfg KafkaConsumerConfig) (EventConsumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("grnoti/kafka: KafkaConsumerConfig.Brokers is required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("grnoti/kafka: KafkaConsumerConfig.GroupID is required")
	}
	if len(cfg.Topics) == 0 {
		return nil, fmt.Errorf("grnoti/kafka: KafkaConsumerConfig.Topics is required")
	}
	logger := OrNop(cfg.Logger)

	saramaCfg := cfg.SaramaConfig
	if saramaCfg == nil {
		saramaCfg = sarama.NewConfig()
		saramaCfg.Version = sarama.V3_0_0_0
		saramaCfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		saramaCfg.Consumer.Return.Errors = true
	}

	consumerGroup, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaCfg)
	if err != nil {
		logger.Error("grnoti/kafka: create consumer group failed", "group_id", cfg.GroupID, "error", err)
		return nil, fmt.Errorf("grnoti/kafka: create consumer group: %w", ErrBackendUnavailable)
	}

	logger.Info("grnoti/kafka: consumer group created", "group_id", cfg.GroupID, "topics", cfg.Topics)
	return &kafkaEventConsumer{
		consumerGroup: consumerGroup,
		topics:        cfg.Topics,
		logger:        logger,
		ready:         make(chan struct{}),
	}, nil
}

// Start begins consuming and invoking handler for each decoded Event,
// blocking until ctx is canceled or the consumer group returns an
// unrecoverable error. Returns nil on ctx cancellation (expected shutdown),
// the wrapped error otherwise.
func (c *kafkaEventConsumer) Start(ctx context.Context, handler func(context.Context, Event) error) error {
	if c.closed.Load() {
		return ErrClosed
	}
	c.mu.Lock()
	c.handler = handler
	c.mu.Unlock()

	go func() {
		for err := range c.consumerGroup.Errors() {
			c.logger.Error("grnoti/kafka: consumer group error", "error", err)
		}
	}()

	c.logger.Info("grnoti/kafka: consumer starting", "topics", c.topics)
	for {
		if err := ctx.Err(); err != nil {
			c.logger.Info("grnoti/kafka: context done, stopping consumer")
			return nil
		}

		// Consume blocks for one session (until rebalance or ctx cancel).
		if err := c.consumerGroup.Consume(ctx, c.topics, c); err != nil {
			if c.closed.Load() {
				return nil
			}
			c.logger.Error("grnoti/kafka: consume error", "error", err)
			return fmt.Errorf("grnoti/kafka: consume: %w", err)
		}
		if c.closed.Load() {
			return nil
		}
	}
}

// Close stops consuming and releases the underlying consumer group.
// Idempotent.
func (c *kafkaEventConsumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if cerr := c.consumerGroup.Close(); cerr != nil {
			err = fmt.Errorf("grnoti/kafka: close consumer group: %w", cerr)
		}
		c.logger.Info("grnoti/kafka: consumer closed")
	})
	return err
}

// Setup is called by sarama at the start of a new consumer-group session.
func (c *kafkaEventConsumer) Setup(sarama.ConsumerGroupSession) error {
	c.readyOnce.Do(func() { close(c.ready) })
	return nil
}

// Cleanup is called by sarama at the end of a consumer-group session.
func (c *kafkaEventConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim processes messages from one partition claim, decoding each
// as an Event and invoking the configured handler.
func (c *kafkaEventConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.mu.Lock()
	handler := c.handler
	c.mu.Unlock()

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			var event Event
			if err := json.Unmarshal(message.Value, &event); err != nil {
				c.logger.Error("grnoti/kafka: unmarshal event failed", "topic", message.Topic, "offset", message.Offset, "error", err)
				// Mark it anyway: a poison message that will never parse
				// would otherwise block this partition forever.
				session.MarkMessage(message, "")
				continue
			}

			if handler != nil {
				if err := handler(session.Context(), event); err != nil {
					c.logger.Error("grnoti/kafka: handler error", "event_id", event.EventID, "error", err)
					// Don't mark: let it be redelivered.
					continue
				}
			}
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// WaitReady blocks until the consumer group has completed its first
// Setup (i.e. joined the group and been assigned partitions), or ctx is
// done. Useful in tests to avoid publishing before the consumer is
// actually subscribed.
func (c *kafkaEventConsumer) WaitReady(ctx context.Context) error {
	select {
	case <-c.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
