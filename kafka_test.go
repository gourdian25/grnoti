// File: kafka_test.go

package grnoti

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

var testKafkaBrokers = []string{"localhost:9092"}

// testKafkaTopic derives a run-unique topic name from the currently
// executing test and explicitly creates it via the admin API — relying on
// auto.create.topics.enable would work too, but doing it explicitly avoids
// the metadata-propagation race a freshly-auto-created topic has on its
// very first produce/consume, and matches this repo's existing preference
// for explicit setup (e.g. Mongo's explicit index creation) over implicit
// backend behavior. Per-test uniqueness follows the same reasoning as the
// Mongo contract tests' per-test collection names (contract_helpers_test.go)
// and Stage 8's discovery that a name derived only from t.Name() collides
// across separate `go test` invocations against a real, persistent broker.
func testKafkaTopic(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("grnoti-test-%s-%d", t.Name(), time.Now().UnixNano())

	admin, err := sarama.NewClusterAdmin(testKafkaBrokers, nil)
	if err != nil {
		t.Skipf("Kafka not available at %v, skipping: %v", testKafkaBrokers, err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(name, &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Skipf("Kafka not available or topic creation failed, skipping: %v", err)
	}
	return name
}

func rawSaramaProducer(t *testing.T) sarama.SyncProducer {
	t.Helper()
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(testKafkaBrokers, cfg)
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	t.Cleanup(func() { _ = producer.Close() })
	return producer
}

// --- producer.kafka.go ---

func TestNewKafkaAnalyticsPublisher_EmptyBrokers(t *testing.T) {
	if _, err := NewKafkaAnalyticsPublisher(KafkaAnalyticsPublisherConfig{}); err == nil {
		t.Fatal("NewKafkaAnalyticsPublisher(empty Brokers) = nil error, want non-nil")
	}
}

func TestNewKafkaAnalyticsPublisher_UnreachableBroker(t *testing.T) {
	_, err := NewKafkaAnalyticsPublisher(KafkaAnalyticsPublisherConfig{Brokers: []string{"127.0.0.1:1"}})
	if err == nil {
		t.Fatal("NewKafkaAnalyticsPublisher(unreachable broker) = nil error, want non-nil")
	}
}

func TestNewKafkaEventConsumer_EmptyBrokers(t *testing.T) {
	_, err := NewKafkaEventConsumer(KafkaConsumerConfig{GroupID: "g", Topics: []string{"t"}})
	if err == nil {
		t.Fatal("NewKafkaEventConsumer(empty Brokers) = nil error, want non-nil")
	}
}

func TestNewKafkaEventConsumer_EmptyGroupID(t *testing.T) {
	_, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, Topics: []string{"t"}})
	if err == nil {
		t.Fatal("NewKafkaEventConsumer(empty GroupID) = nil error, want non-nil")
	}
}

func TestNewKafkaEventConsumer_EmptyTopics(t *testing.T) {
	_, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: "g"})
	if err == nil {
		t.Fatal("NewKafkaEventConsumer(empty Topics) = nil error, want non-nil")
	}
}

func TestKafkaAnalyticsPublisher_PublishImpressionAndConversion(t *testing.T) {
	impressionTopic := testKafkaTopic(t)
	conversionTopic := testKafkaTopic(t)

	publisher, err := NewKafkaAnalyticsPublisher(KafkaAnalyticsPublisherConfig{
		Brokers: testKafkaBrokers, ImpressionTopic: impressionTopic, ConversionTopic: conversionTopic,
	})
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	defer publisher.Close()
	ctx := context.Background()

	if err := publisher.PublishImpression(ctx, "u1", "exp1", "variant-a"); err != nil {
		t.Fatalf("PublishImpression: %v", err)
	}
	if err := publisher.PublishConversion(ctx, "u1", "exp1"); err != nil {
		t.Fatalf("PublishConversion: %v", err)
	}

	impMsg := readOneMessage(t, impressionTopic)
	var impPayload experimentImpressionMessage
	if err := json.Unmarshal(impMsg, &impPayload); err != nil {
		t.Fatalf("unmarshal impression message: %v", err)
	}
	if impPayload.UserID != "u1" || impPayload.ExperimentID != "exp1" || impPayload.VariantID != "variant-a" {
		t.Fatalf("impression payload = %+v, want UserID=u1 ExperimentID=exp1 VariantID=variant-a", impPayload)
	}

	convMsg := readOneMessage(t, conversionTopic)
	var convPayload experimentConversionMessage
	if err := json.Unmarshal(convMsg, &convPayload); err != nil {
		t.Fatalf("unmarshal conversion message: %v", err)
	}
	if convPayload.UserID != "u1" || convPayload.ExperimentID != "exp1" {
		t.Fatalf("conversion payload = %+v, want UserID=u1 ExperimentID=exp1", convPayload)
	}
}

func TestKafkaAnalyticsPublisher_Close_Idempotent(t *testing.T) {
	publisher, err := NewKafkaAnalyticsPublisher(KafkaAnalyticsPublisherConfig{Brokers: testKafkaBrokers, ImpressionTopic: testKafkaTopic(t)})
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := publisher.Close(); err != nil {
		t.Fatalf("second Close: %v, want nil", err)
	}
	if err := publisher.PublishImpression(context.Background(), "u1", "exp1", "a"); err != ErrClosed {
		t.Fatalf("PublishImpression after Close error = %v, want ErrClosed", err)
	}
}

// readOneMessage consumes exactly one message from topic's single partition
// starting at the oldest offset, via a plain (non-group) sarama.Consumer —
// deliberately not reusing kafkaEventConsumer here, to keep this an
// independent check of what NewKafkaAnalyticsPublisher actually put on the
// wire.
func readOneMessage(t *testing.T, topic string) []byte {
	t.Helper()
	consumer, err := sarama.NewConsumer(testKafkaBrokers, nil)
	if err != nil {
		t.Fatalf("sarama.NewConsumer: %v", err)
	}
	defer consumer.Close()

	partConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("ConsumePartition(%s): %v", topic, err)
	}
	defer partConsumer.Close()

	select {
	case msg := <-partConsumer.Messages():
		return msg.Value
	case <-time.After(15 * time.Second):
		t.Fatalf("timed out waiting for a message on topic %s", topic)
		return nil
	}
}

// --- consumer.kafka.go ---

func TestKafkaEventConsumer_PublishAndConsume(t *testing.T) {
	topic := testKafkaTopic(t)
	producer := rawSaramaProducer(t)

	consumer, err := NewKafkaEventConsumer(KafkaConsumerConfig{
		Brokers: testKafkaBrokers, GroupID: "grnoti-test-group-" + topic, Topics: []string{topic},
	})
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	defer consumer.Close()

	var mu sync.Mutex
	var received []Event
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- consumer.Start(ctx, func(_ context.Context, e Event) error {
			mu.Lock()
			received = append(received, e)
			mu.Unlock()
			return nil
		})
	}()

	if err := consumer.(*kafkaEventConsumer).WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	payload, _ := json.Marshal(Event{EventID: "evt-1", Type: EventTypeSystemAlert, UserID: "u1"})
	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(payload)}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	waitForCondition(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 1
	})

	mu.Lock()
	defer mu.Unlock()
	if received[0].EventID != "evt-1" {
		t.Fatalf("received[0].EventID = %q, want evt-1", received[0].EventID)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Start returned error after ctx cancel: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return within 10s of ctx cancellation")
	}
}

func TestKafkaEventConsumer_PoisonMessageDoesNotBlockPartition(t *testing.T) {
	topic := testKafkaTopic(t)
	producer := rawSaramaProducer(t)

	consumer, err := NewKafkaEventConsumer(KafkaConsumerConfig{
		Brokers: testKafkaBrokers, GroupID: "grnoti-test-group-" + topic, Topics: []string{topic},
	})
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	defer consumer.Close()

	var mu sync.Mutex
	var received []Event
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = consumer.Start(ctx, func(_ context.Context, e Event) error {
			mu.Lock()
			received = append(received, e)
			mu.Unlock()
			return nil
		})
	}()
	if err := consumer.(*kafkaEventConsumer).WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	// Poison message: not valid JSON at all.
	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder([]byte("not-json"))}); err != nil {
		t.Fatalf("SendMessage(poison): %v", err)
	}
	validPayload, _ := json.Marshal(Event{EventID: "evt-after-poison"})
	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(validPayload)}); err != nil {
		t.Fatalf("SendMessage(valid): %v", err)
	}

	waitForCondition(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) == 1
	})
	mu.Lock()
	defer mu.Unlock()
	if received[0].EventID != "evt-after-poison" {
		t.Fatalf("received[0].EventID = %q, want evt-after-poison (the poison message before it must not have blocked delivery)", received[0].EventID)
	}
}

// TestKafkaEventConsumer_HandlerErrorRedeliversAfterRestart proves the
// at-least-once contract: an Event whose handler returns an error is not
// marked, so its offset is never committed, so a fresh consumer in the same
// group picks it up again — the real-world scenario is a process crash
// mid-handler, simulated here by closing and recreating the consumer.
func TestKafkaEventConsumer_HandlerErrorRedeliversAfterRestart(t *testing.T) {
	topic := testKafkaTopic(t)
	producer := rawSaramaProducer(t)
	groupID := "grnoti-test-group-" + topic

	payload, _ := json.Marshal(Event{EventID: "evt-retry"})
	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(payload)}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// First consumer: handler always fails, so the message is never marked.
	consumer1, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: groupID, Topics: []string{topic}})
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	var attempts1 atomic.Int32
	ctx1, cancel1 := context.WithCancel(context.Background())
	go func() {
		_ = consumer1.Start(ctx1, func(context.Context, Event) error {
			attempts1.Add(1)
			return fmt.Errorf("simulated processing failure")
		})
	}()
	if err := consumer1.(*kafkaEventConsumer).WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	waitForCondition(t, 15*time.Second, func() bool { return attempts1.Load() >= 1 })
	cancel1()
	_ = consumer1.Close()

	// Second consumer, same group: must still see evt-retry since it was
	// never committed.
	consumer2, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: groupID, Topics: []string{topic}})
	if err != nil {
		t.Fatalf("NewKafkaEventConsumer (second): %v", err)
	}
	defer consumer2.Close()
	var mu sync.Mutex
	var received []string
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() {
		_ = consumer2.Start(ctx2, func(_ context.Context, e Event) error {
			mu.Lock()
			received = append(received, e.EventID)
			mu.Unlock()
			return nil
		})
	}()

	waitForCondition(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(received) >= 1
	})
	mu.Lock()
	defer mu.Unlock()
	if received[0] != "evt-retry" {
		t.Fatalf("second consumer received %v, want [evt-retry] redelivered", received)
	}
}

// TestKafkaEventConsumer_WiresToWorkerPool proves the intended composition
// (docs/plan/grnoti-plan.md §3.1, Stage 9/12): a consumer handler that does
// nothing but pool.Submit correctly bridges Kafka ingestion into the
// WorkerPool's bounded queue and out to pool workers.
func TestKafkaEventConsumer_WiresToWorkerPool(t *testing.T) {
	topic := testKafkaTopic(t)
	producer := rawSaramaProducer(t)

	consumer, err := NewKafkaEventConsumer(KafkaConsumerConfig{
		Brokers: testKafkaBrokers, GroupID: "grnoti-test-group-" + topic, Topics: []string{topic},
	})
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	defer consumer.Close()

	var mu sync.Mutex
	var processed []string
	pool, err := NewWorkerPool(WorkerPoolDeps{
		Config: WorkerPoolConfig{Workers: 2, QueueSize: 10},
		Handler: func(_ context.Context, e Event) error {
			mu.Lock()
			processed = append(processed, e.EventID)
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewWorkerPool: %v", err)
	}
	pool.Start()
	defer pool.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = consumer.Start(ctx, func(_ context.Context, e Event) error {
			return pool.Submit(e)
		})
	}()
	if err := consumer.(*kafkaEventConsumer).WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	const numEvents = 5
	for i := 0; i < numEvents; i++ {
		payload, _ := json.Marshal(Event{EventID: fmt.Sprintf("evt-%d", i)})
		if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(payload)}); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}

	waitForCondition(t, 15*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) == numEvents
	})
}

// TestKafkaEventConsumer_WiresToNotificationService is the end-to-end proof
// of docs/plan/grnoti-plan.md §3.1's stated fix, using the real
// NotificationService (Stage 12) rather than a bare handler stub as
// TestKafkaEventConsumer_WiresToWorkerPool above does: consumer.Start(ctx,
// service.Submit) is the entire ingestion bridge — Submit internally
// enqueues onto the service's own WorkerPool (EnableBackpressure) and a
// pool worker calls back into the same ProcessEvent pipeline that marks
// idempotency, dispatches, and would DLQ/publish lifecycle events.
func TestKafkaEventConsumer_WiresToNotificationService(t *testing.T) {
	topic := testKafkaTopic(t)
	producer := rawSaramaProducer(t)

	consumer, err := NewKafkaEventConsumer(KafkaConsumerConfig{
		Brokers: testKafkaBrokers, GroupID: "grnoti-test-group-" + topic, Topics: []string{topic},
	})
	if err != nil {
		t.Skipf("Kafka not available, skipping: %v", err)
	}
	defer consumer.Close()

	tokenStore := NewMemoryTokenStore()
	_ = tokenStore.SaveToken(context.Background(), DeviceToken{Token: "t1", UserID: "u1", Platform: PlatformAndroid})
	idempotency := newTestIdempotencyStore(t)

	svc, err := NewNotificationService(ServiceDeps{
		TokenStore:  tokenStore,
		Dispatcher:  &stubDispatcher{},
		Templates:   NewTemplateEngine(),
		Idempotency: idempotency,
		Config: ServiceConfig{
			IdempotencyTTL:     time.Hour,
			EnableBackpressure: true,
		},
		WorkerPoolConfig: WorkerPoolConfig{Workers: 2, QueueSize: 10},
	})
	if err != nil {
		t.Fatalf("NewNotificationService: %v", err)
	}
	defer svc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = consumer.Start(ctx, svc.Submit)
	}()
	if err := consumer.(*kafkaEventConsumer).WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	event := Event{EventID: "kafka-e2e-1", UserID: "u1", Type: EventTypeSystemAlert, Priority: PriorityNormal}
	payload, _ := json.Marshal(event)
	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(payload)}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	waitForCondition(t, 15*time.Second, func() bool {
		processed, _ := idempotency.IsProcessed(context.Background(), "kafka-e2e-1")
		return processed
	})
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within timeout")
	}
}
