// File: producer.kafka.go

package grnoti

import (
	"context"
	"encoding/json"
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
			err = fmt.Errorf("grnoti/kafka: close producer: %w", cerr)
		}
		p.logger.Info("grnoti/kafka: analytics publisher closed")
	})
	return err
}
