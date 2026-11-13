// File: consumer.kafka.go

package grnoti

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

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
	// DLQProducer writes a message the consumer can't handle to its
	// dead-letter topic, KafkaDLQTopic(<its topic>). Required: without it a
	// message that fails could only be dropped or block its partition. It is
	// the caller's and never closed by grnoti; make it idempotent
	// (Producer.Idempotent, RequiredAcks WaitForAll) so a retried send never
	// writes twice, and create each dead-letter topic beside its topic.
	DLQProducer sarama.SyncProducer
	// Retry is how hard a message is handled before it is dead-lettered. The
	// zero value is DefaultConsumerRetryPolicy().
	Retry ConsumerRetryPolicy
	// SaramaConfig, if nil, defaults to sarama.V3_0_0_0, round-robin
	// rebalancing, OffsetOldest and Consumer.Return.Errors=true.
	SaramaConfig *sarama.Config
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

// KafkaDLQSuffix names a topic's dead-letter topic: "<topic>.dlq".
const KafkaDLQSuffix = ".dlq"

// KafkaDLQTopic is topic's dead-letter topic.
func KafkaDLQTopic(topic string) string { return topic + KafkaDLQSuffix }

// The headers a dead-lettered message carries beside its own.
const (
	KafkaHeaderError     = "grnoti-error"
	KafkaHeaderTopic     = "grnoti-original-topic"
	KafkaHeaderPartition = "grnoti-original-partition"
	KafkaHeaderOffset    = "grnoti-original-offset"
	KafkaHeaderAttempts  = "grnoti-attempts"
)

// ConsumerRetryPolicy is how hard the consumer handles a message before
// dead-lettering it.
type ConsumerRetryPolicy struct {
	// Attempts is how many times a message is handled before it is
	// dead-lettered, the first included: at least 1.
	Attempts int
	// Backoff is the wait after the first failure, doubled after each
	// further one, never more than MaxBackoff.
	Backoff    time.Duration
	MaxBackoff time.Duration
	// HaltPause is how long a stopped session (a dead-letter write failed)
	// waits before the next one starts from the committed offset.
	HaltPause time.Duration
}

// DefaultConsumerRetryPolicy is five attempts over about 7.5 s, then the
// dead-letter topic; a stopped partition starts again after 10 s.
func DefaultConsumerRetryPolicy() ConsumerRetryPolicy {
	return ConsumerRetryPolicy{Attempts: 5, Backoff: 500 * time.Millisecond, MaxBackoff: 30 * time.Second, HaltPause: 10 * time.Second}
}

func (p ConsumerRetryPolicy) validate() error {
	switch {
	case p.Attempts < 1:
		return fmt.Errorf("grnoti/kafka: ConsumerRetryPolicy.Attempts must be at least 1, got %d", p.Attempts)
	case p.Backoff <= 0 || p.MaxBackoff < p.Backoff:
		return fmt.Errorf("grnoti/kafka: ConsumerRetryPolicy.Backoff must be positive and at most MaxBackoff, got %s and %s", p.Backoff, p.MaxBackoff)
	case p.HaltPause <= 0:
		return fmt.Errorf("grnoti/kafka: ConsumerRetryPolicy.HaltPause must be positive, got %s", p.HaltPause)
	}
	return nil
}

// wait is the backoff after the attempt-th failure (1-based).
func (p ConsumerRetryPolicy) wait(attempt int) time.Duration {
	d := p.Backoff
	for i := 1; i < attempt && d < p.MaxBackoff; i++ {
		d *= 2
	}
	return min(d, p.MaxBackoff)
}

// kafkaEventConsumer implements EventConsumer using Kafka via
// github.com/IBM/sarama's consumer-group API.
//
// A consumer group commits one offset per partition: marking message n + 1
// commits every message before it. So a loop that skips a message it failed
// on and marks the next one has lost it for good (DS-09, the defect v0.5.x
// had). This one never does. Each message is:
//
//  1. decoded into an Event; one that won't decode never will, so it goes
//     straight to step 3;
//  2. handled, and handled again with backoff (Retry) while it fails;
//  3. if it still fails, written to KafkaDLQTopic(<its topic>) as it came,
//     with why and where from in headers (KafkaHeader*);
//  4. only then marked.
//
// A dead-letter write that fails stops the partition there: the session ends
// unmarked past the message, and after Retry.HaltPause a new one starts from
// the committed offset, so the message is handled again. A session that ends
// mid-retry (a rebalance, or ctx done) leaves the message unmarked too. A
// handler answering ErrClosed (the service has shut down, and will take
// nothing again) leaves the message unmarked, never dead-lettered, and ends
// Start with that error rather than rejoining to be refused again. Delivery is therefore at least once: a handler must be
// idempotent (NotificationService is, by EventID).
//
// It logs through grnoti's own Logger interface rather than a concrete
// *grlog.Logger, so a caller can plug in any logger.
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
	dlq           sarama.SyncProducer
	policy        ConsumerRetryPolicy
	logger        Logger

	ready     chan struct{}
	readyOnce sync.Once

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ EventConsumer = (*kafkaEventConsumer)(nil)

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
	if cfg.DLQProducer == nil {
		return nil, fmt.Errorf("grnoti/kafka: KafkaConsumerConfig.DLQProducer is required")
	}
	policy := cfg.Retry
	if policy == (ConsumerRetryPolicy{}) {
		policy = DefaultConsumerRetryPolicy()
	}
	if err := policy.validate(); err != nil {
		return nil, err
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
		dlq:           cfg.DLQProducer,
		policy:        policy,
		logger:        logger,
		ready:         make(chan struct{}),
	}, nil
}

// Start begins consuming and invoking handler for each decoded Event under
// the policy above, blocking until ctx is canceled, the consumer is closed,
// or the consumer group returns an unrecoverable error. Returns nil on ctx
// cancellation or Close (expected shutdown), the wrapped error otherwise.
func (c *kafkaEventConsumer) Start(ctx context.Context, handler func(context.Context, Event) error) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if handler == nil {
		return fmt.Errorf("grnoti/kafka: Start needs a handler")
	}

	go func() {
		for err := range c.consumerGroup.Errors() {
			c.logger.Error("grnoti/kafka: consumer group error", "error", err)
		}
	}()

	c.logger.Info("grnoti/kafka: consumer starting", "topics", c.topics)
	for {
		if ctx.Err() != nil {
			c.logger.Info("grnoti/kafka: context done, stopping consumer")
			return nil
		}

		// Consume blocks for one session (until a rebalance, ctx done, or a
		// halt ends it).
		sessionCtx, endSession := context.WithCancel(ctx)
		session := &consumerSession{consumer: c, handler: handler, halt: endSession}
		err := c.consumerGroup.Consume(sessionCtx, c.topics, session)
		endSession()
		if c.closed.Load() || ctx.Err() != nil {
			return nil
		}
		if session.handlerClosed.Load() {
			c.logger.Warn("grnoti/kafka: the handler is closed; stopping the consumer with its message unmarked")
			return fmt.Errorf("grnoti/kafka: the handler is closed: %w", ErrClosed)
		}
		if err != nil && !session.halted.Load() {
			// Deliberately untested: reaching this needs a fake
			// sarama.ConsumerGroup whose Consume fails while the consumer
			// is still open (docs/architecture.md §6).
			c.logger.Error("grnoti/kafka: consume error", "error", err)
			return fmt.Errorf("grnoti/kafka: consume: %w", err)
		}
		if !session.halted.Load() {
			continue // a rebalance: rejoin at once
		}
		c.logger.Warn("grnoti/kafka: stopped at a message that could be neither handled nor dead-lettered; starting again from the committed offset",
			"pause", c.policy.HaltPause)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(c.policy.HaltPause):
		}
	}
}

// Close stops consuming and releases the underlying consumer group (never
// DLQProducer, which is the caller's). Idempotent.
func (c *kafkaEventConsumer) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		if cerr := c.consumerGroup.Close(); cerr != nil {
			// Deliberately untested: needs a fake sarama.ConsumerGroup whose
			// Close fails (docs/architecture.md §6).
			err = fmt.Errorf("grnoti/kafka: close consumer group: %w", cerr)
		}
		c.logger.Info("grnoti/kafka: consumer closed")
	})
	return err
}

// consumerSession is one consumer-group session's sarama handler.
type consumerSession struct {
	consumer *kafkaEventConsumer
	handler  func(context.Context, Event) error
	// halt ends the session: a dead-letter write failed.
	halt   context.CancelFunc
	halted atomic.Bool
	// handlerClosed is a handler that answered ErrClosed: the session ends
	// and Start stops.
	handlerClosed atomic.Bool
}

var _ sarama.ConsumerGroupHandler = (*consumerSession)(nil)

// Setup is called by sarama at the start of a new consumer-group session.
func (s *consumerSession) Setup(sarama.ConsumerGroupSession) error {
	s.consumer.readyOnce.Do(func() { close(s.consumer.ready) })
	return nil
}

// Cleanup is called by sarama at the end of a consumer-group session.
func (s *consumerSession) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// errHalted is ConsumeClaim's answer when it stopped at a message it could
// neither handle nor dead-letter.
var errHalted = errors.New("grnoti/kafka: stopped at a message that could be neither handled nor dead-lettered")

// ConsumeClaim processes one partition's messages in order, each handled
// (or dead-lettered) before it is marked.
func (s *consumerSession) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	ctx := session.Context()
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			done, err := s.process(ctx, message)
			if err != nil {
				s.halted.Store(true)
				s.halt()
				return err
			}
			if !done {
				if s.handlerClosed.Load() {
					s.halt()
				}
				return nil // the session ended mid-retry, or the handler closed: unmarked
			}
			session.MarkMessage(message, "")
		}
	}
}

// process handles message under the policy, dead-lettering it when it won't
// decode or every attempt fails. done is whether message may be marked; err
// is a dead-letter write that failed.
func (s *consumerSession) process(ctx context.Context, message *sarama.ConsumerMessage) (done bool, err error) {
	c := s.consumer
	var event Event
	if err := json.Unmarshal(message.Value, &event); err != nil {
		c.logger.Error("grnoti/kafka: unmarshal event failed", "topic", message.Topic, "offset", message.Offset, "error", err)
		return s.deadLetter(message, fmt.Errorf("decode: %w", err), 0)
	}

	var lastErr error
	for attempt := 1; attempt <= c.policy.Attempts; attempt++ {
		lastErr = s.call(ctx, event)
		if lastErr == nil {
			return true, nil
		}
		if errors.Is(lastErr, ErrClosed) {
			c.logger.Info("grnoti/kafka: handler closed; leaving the message unmarked", "event_id", event.EventID)
			s.handlerClosed.Store(true)
			return false, nil
		}
		c.logger.Warn("grnoti/kafka: handler error", "event_id", event.EventID, "attempt", attempt, "attempts", c.policy.Attempts, "error", lastErr)
		if attempt == c.policy.Attempts {
			break
		}
		select {
		case <-ctx.Done():
			return false, nil
		case <-time.After(c.policy.wait(attempt)):
		}
	}
	if ctx.Err() != nil {
		return false, nil //nolint:nilerr // the session ended: the message stays unmarked, which is no failure
	}
	return s.deadLetter(message, lastErr, c.policy.Attempts)
}

// call runs the handler, a panic coming back as an error that names its type
// alone: its value could hold what the event carries.
func (s *consumerSession) call(ctx context.Context, event Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.consumer.logger.Error("grnoti/kafka: handler panicked", "event_id", event.EventID, "type", fmt.Sprintf("%T", r), "stack", string(debug.Stack()))
			err = fmt.Errorf("grnoti/kafka: the handler panicked (%T)", r)
		}
	}()
	return s.handler(ctx, event)
}

// deadLetter writes message to its dead-letter topic: the same key, value
// and headers, plus why (cause's text, which must hold no secret) and where
// from. done is true once it is written; a failed write is the error that
// stops the partition.
func (s *consumerSession) deadLetter(message *sarama.ConsumerMessage, cause error, attempts int) (done bool, err error) {
	headers := make([]sarama.RecordHeader, 0, len(message.Headers)+5)
	for _, h := range message.Headers {
		if h != nil {
			headers = append(headers, *h)
		}
	}
	headers = append(headers,
		sarama.RecordHeader{Key: []byte(KafkaHeaderError), Value: []byte(cause.Error())},
		sarama.RecordHeader{Key: []byte(KafkaHeaderTopic), Value: []byte(message.Topic)},
		sarama.RecordHeader{Key: []byte(KafkaHeaderPartition), Value: []byte(strconv.Itoa(int(message.Partition)))},
		sarama.RecordHeader{Key: []byte(KafkaHeaderOffset), Value: []byte(strconv.FormatInt(message.Offset, 10))},
		sarama.RecordHeader{Key: []byte(KafkaHeaderAttempts), Value: []byte(strconv.Itoa(attempts))},
	)
	if _, _, err := s.consumer.dlq.SendMessage(&sarama.ProducerMessage{
		Topic:   KafkaDLQTopic(message.Topic),
		Key:     sarama.ByteEncoder(message.Key),
		Value:   sarama.ByteEncoder(message.Value),
		Headers: headers,
	}); err != nil {
		s.consumer.logger.Error("grnoti/kafka: dead-lettering a message failed; stopping its partition there",
			"topic", message.Topic, "partition", message.Partition, "offset", message.Offset, "error", err)
		return false, fmt.Errorf("%w: %s[%d]@%d: %w", errHalted, message.Topic, message.Partition, message.Offset, err)
	}
	s.consumer.logger.Error("grnoti/kafka: message dead-lettered",
		"topic", message.Topic, "dlq", KafkaDLQTopic(message.Topic), "partition", message.Partition, "offset", message.Offset)
	return true, nil
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
