// File: producer.kafka.go

package grnoti

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

const (
	// DefaultImpressionTopic is used when
	// KafkaAnalyticsPublisherConfig.ImpressionTopic is empty.
	DefaultImpressionTopic = "grnoti.experiment.impressions"
	// DefaultConversionTopic is used when
	// KafkaAnalyticsPublisherConfig.ConversionTopic is empty.
	DefaultConversionTopic = "grnoti.experiment.conversions"
)

// experimentImpressionMessage is the JSON wire format published to
// ImpressionTopic.
type experimentImpressionMessage struct {
	UserID       string    `json:"user_id"`
	ExperimentID string    `json:"experiment_id"`
	VariantID    string    `json:"variant_id"`
	Timestamp    time.Time `json:"timestamp"`
}

// experimentConversionMessage is the JSON wire format published to
// ConversionTopic.
type experimentConversionMessage struct {
	UserID       string    `json:"user_id"`
	ExperimentID string    `json:"experiment_id"`
	Timestamp    time.Time `json:"timestamp"`
}

// KafkaAnalyticsPublisherConfig configures an AnalyticsPublisher
// constructed by NewKafkaAnalyticsPublisher.
type KafkaAnalyticsPublisherConfig struct {
	// Brokers is the Kafka bootstrap broker list. Required.
	Brokers []string
	// ImpressionTopic defaults to DefaultImpressionTopic if empty.
	ImpressionTopic string
	// ConversionTopic defaults to DefaultConversionTopic if empty.
	ConversionTopic string
	// SaramaConfig, if nil, defaults to a config with
	// Producer.Return.Successes=true (required for SyncProducer) and
	// Producer.RequiredAcks=WaitForLocal. A caller-supplied SaramaConfig
	// must set Producer.Return.Successes itself — sarama.NewSyncProducer
	// errors explicitly if it doesn't, rather than this constructor
	// silently patching caller-owned config.
	SaramaConfig *sarama.Config
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

// kafkaAnalyticsPublisher is a Kafka-backed AnalyticsPublisher — new
// relative to the reference implementation, whose TrackImpression/
// TrackConversion were hardcoded no-ops (docs/plan/grnoti-plan.md §2 item
// 9). Messages are keyed by userID so one user's impression/conversion
// events land on the same partition, giving per-user ordering.
type kafkaAnalyticsPublisher struct {
	producer        sarama.SyncProducer
	impressionTopic string
	conversionTopic string
	logger          Logger

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ AnalyticsPublisher = (*kafkaAnalyticsPublisher)(nil)

// NewKafkaAnalyticsPublisher builds a sarama.SyncProducer from cfg —
// synchronous rather than async so PublishImpression/PublishConversion's
// error return reflects an actual broker ack, not just a local enqueue.
func NewKafkaAnalyticsPublisher(cfg KafkaAnalyticsPublisherConfig) (AnalyticsPublisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("grnoti/kafka: KafkaAnalyticsPublisherConfig.Brokers is required")
	}
	impressionTopic := cfg.ImpressionTopic
	if impressionTopic == "" {
		impressionTopic = DefaultImpressionTopic
	}
	conversionTopic := cfg.ConversionTopic
	if conversionTopic == "" {
		conversionTopic = DefaultConversionTopic
	}
	logger := OrNop(cfg.Logger)

	saramaCfg := cfg.SaramaConfig
	if saramaCfg == nil {
		saramaCfg = sarama.NewConfig()
		saramaCfg.Version = sarama.V3_0_0_0
		saramaCfg.Producer.Return.Successes = true
		saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	}

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		logger.Error("grnoti/kafka: create sync producer failed", "error", err)
		return nil, fmt.Errorf("grnoti/kafka: create sync producer: %w", ErrBackendUnavailable)
	}

	logger.Info("grnoti/kafka: analytics publisher connected", "impression_topic", impressionTopic, "conversion_topic", conversionTopic)
	return &kafkaAnalyticsPublisher{
		producer:        producer,
		impressionTopic: impressionTopic,
		conversionTopic: conversionTopic,
		logger:          logger,
	}, nil
}

func (p *kafkaAnalyticsPublisher) PublishImpression(ctx context.Context, userID, experimentID, variantID string) error {
	if p.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(experimentImpressionMessage{
		UserID: userID, ExperimentID: experimentID, VariantID: variantID, Timestamp: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("grnoti/kafka: marshal impression: %w", err)
	}
	if _, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.impressionTopic,
		Key:   sarama.StringEncoder(userID),
		Value: sarama.ByteEncoder(payload),
	}); err != nil {
		return fmt.Errorf("grnoti/kafka: publish impression: %w", ErrBackendUnavailable)
	}
	return nil
}

func (p *kafkaAnalyticsPublisher) PublishConversion(ctx context.Context, userID, experimentID string) error {
	if p.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(experimentConversionMessage{
		UserID: userID, ExperimentID: experimentID, Timestamp: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("grnoti/kafka: marshal conversion: %w", err)
	}
	if _, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.conversionTopic,
		Key:   sarama.StringEncoder(userID),
		Value: sarama.ByteEncoder(payload),
	}); err != nil {
		return fmt.Errorf("grnoti/kafka: publish conversion: %w", ErrBackendUnavailable)
	}
	return nil
}

// Close closes the underlying sarama.SyncProducer. Idempotent.
func (p *kafkaAnalyticsPublisher) Close() error {
	var err error
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		if cerr := p.producer.Close(); cerr != nil {
			// Deliberately untested: needs a fake sarama.SyncProducer whose
			// Close fails (docs/architecture.md §6).
			err = fmt.Errorf("grnoti/kafka: close producer: %w", cerr)
		}
		p.logger.Info("grnoti/kafka: analytics publisher closed")
	})
	return err
}

// KafkaProducerConfig configures an EventProducer constructed by
// NewKafkaEventProducer.
type KafkaProducerConfig struct {
	// Producer sends every message. Required. It is the caller's: build it
	// with sarama.NewSyncProducer (or share one the application already
	// has), choose its acks/retries/idempotence settings, and close it
	// yourself — grnoti never closes it.
	Producer sarama.SyncProducer
	// Topic is where every event is published, normally one of the
	// KafkaConsumerConfig.Topics a NewKafkaEventConsumer reads. Required.
	Topic string
	// Logger receives optional diagnostic messages. A nil Logger disables
	// logging.
	Logger Logger
}

// kafkaEventProducer is the Kafka-backed EventProducer, the counterpart of
// kafkaEventConsumer: each event is encoded with encoding/json, the format
// ConsumeClaim decodes, and keyed by EventID. Unlike every other backend in
// this package it never owns its connection — the producer is always
// injected — so Close only stops it, and there is no ownership flag to
// track.
type kafkaEventProducer struct {
	producer sarama.SyncProducer
	topic    string
	logger   Logger

	closed    atomic.Bool
	closeOnce sync.Once
}

var _ EventProducer = (*kafkaEventProducer)(nil)

// NewKafkaEventProducer returns an EventProducer that publishes to
// cfg.Topic through cfg.Producer. It does no connectivity check of its own:
// Kafka has no Ping, and an injected producer is already connected.
//
// Parameters:
//   - cfg: KafkaProducerConfig — Producer and Topic are required
//
// Returns:
//   - EventProducer: ready to use
//   - error: non-nil if Producer or Topic is missing
func NewKafkaEventProducer(cfg KafkaProducerConfig) (EventProducer, error) {
	if cfg.Producer == nil {
		return nil, fmt.Errorf("grnoti/kafka: KafkaProducerConfig.Producer is required")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("grnoti/kafka: KafkaProducerConfig.Topic is required")
	}
	logger := OrNop(cfg.Logger)
	logger.Info("grnoti/kafka: event producer ready", "topic", cfg.Topic)
	return &kafkaEventProducer{producer: cfg.Producer, topic: cfg.Topic, logger: logger}, nil
}

// encodeEvent validates event and builds its message: JSON value, EventID
// key. Publish and PublishBatch share it so a batch is checked exactly as
// a single event is.
func (p *kafkaEventProducer) encodeEvent(event Event) (*sarama.ProducerMessage, error) {
	if err := event.Validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return nil, fmt.Errorf("grnoti/kafka: marshal event %s: %w", event.EventID, err)
	}
	return &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(event.EventID),
		Value: sarama.ByteEncoder(payload),
	}, nil
}

// Publish validates and sends event. sarama's SyncProducer takes no
// context, so ctx is checked only before sending; a send already under way
// runs to the producer's own Producer.Timeout.
func (p *kafkaEventProducer) Publish(ctx context.Context, event Event) error {
	if p.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	msg, err := p.encodeEvent(event)
	if err != nil {
		return err
	}
	if _, _, err := p.producer.SendMessage(msg); err != nil {
		return fmt.Errorf("grnoti/kafka: publish event %s: %w", event.EventID, errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

// PublishBatch encodes every event before sending any, so one invalid or
// unencodable event sends nothing; the error names its index and EventID
// and still wraps its sentinel. The batch then goes out in one
// SendMessages call. ctx is checked only before sending, as in Publish.
func (p *kafkaEventProducer) PublishBatch(ctx context.Context, events []Event) error {
	if p.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	msgs := make([]*sarama.ProducerMessage, len(events))
	for i, event := range events {
		msg, err := p.encodeEvent(event)
		if err != nil {
			return fmt.Errorf("grnoti/kafka: batch event %d (%s): %w", i, event.EventID, err)
		}
		msgs[i] = msg
	}
	if err := p.producer.SendMessages(msgs); err != nil {
		return fmt.Errorf("grnoti/kafka: publish batch of %d events: %w", len(events), errors.Join(err, ErrBackendUnavailable))
	}
	return nil
}

// Close stops the producer: every later Publish/PublishBatch returns
// ErrClosed. The injected sarama.SyncProducer is left open — it is the
// caller's to close. Idempotent.
func (p *kafkaEventProducer) Close() error {
	p.closeOnce.Do(func() {
		p.closed.Store(true)
		p.logger.Info("grnoti/kafka: event producer closed", "topic", p.topic)
	})
	return nil
}
