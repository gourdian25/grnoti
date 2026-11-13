// File: consumer.kafka_test.go

package grnoti

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// DS-09: the consumer handles a message before it marks it, retries it,
// dead-letters it, and never commits past one it could neither handle nor
// dead-letter. These run against the shared local broker, each on its own
// topics (testKafkaTopic), and skip without one.

// fastRetry is a policy short enough for a test: three attempts, 50 ms apart.
var fastRetry = ConsumerRetryPolicy{Attempts: 3, Backoff: 50 * time.Millisecond, MaxBackoff: 50 * time.Millisecond, HaltPause: 200 * time.Millisecond}

func TestNewKafkaEventConsumer_RequiresDLQProducer(t *testing.T) {
	_, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: "g", Topics: []string{"t"}})
	if err == nil {
		t.Fatal("NewKafkaEventConsumer(no DLQProducer) = nil error, want a refusal")
	}
}

func TestConsumerRetryPolicy_Validate(t *testing.T) {
	good := DefaultConsumerRetryPolicy()
	if err := good.validate(); err != nil {
		t.Fatalf("DefaultConsumerRetryPolicy().validate() = %v", err)
	}
	for name, p := range map[string]ConsumerRetryPolicy{
		"no attempts":            {Attempts: 0, Backoff: time.Second, MaxBackoff: time.Second, HaltPause: time.Second},
		"no backoff":             {Attempts: 1, Backoff: 0, MaxBackoff: time.Second, HaltPause: time.Second},
		"max below backoff":      {Attempts: 1, Backoff: 2 * time.Second, MaxBackoff: time.Second, HaltPause: time.Second},
		"no halt pause":          {Attempts: 1, Backoff: time.Second, MaxBackoff: time.Second, HaltPause: 0},
		"negative backoff":       {Attempts: 1, Backoff: -time.Second, MaxBackoff: time.Second, HaltPause: time.Second},
		"negative attempts only": {Attempts: -1, Backoff: time.Second, MaxBackoff: time.Second, HaltPause: time.Second},
	} {
		if err := p.validate(); err == nil {
			t.Errorf("%s: validate() = nil, want a refusal", name)
		}
		if _, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: "g", Topics: []string{"t"},
			DLQProducer: failingSyncProducer{}, Retry: p}); err == nil {
			t.Errorf("%s: NewKafkaEventConsumer = nil error, want the policy refused", name)
		}
	}
}

func TestConsumerRetryPolicy_Wait(t *testing.T) {
	p := ConsumerRetryPolicy{Attempts: 9, Backoff: 100 * time.Millisecond, MaxBackoff: 350 * time.Millisecond, HaltPause: time.Second}
	for attempt, want := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 350 * time.Millisecond, 8: 350 * time.Millisecond} {
		if got := p.wait(attempt); got != want {
			t.Errorf("wait(%d) = %s, want %s", attempt, got, want)
		}
	}
	if KafkaDLQTopic("notifications") != "notifications.dlq" {
		t.Errorf("KafkaDLQTopic = %q", KafkaDLQTopic("notifications"))
	}
}

// failingSyncProducer is a DLQ whose every write fails.
type failingSyncProducer struct{ sarama.SyncProducer }

func (failingSyncProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	return 0, 0, errors.New("the dead-letter topic is unreachable")
}

// testKafkaDLQ creates topic's dead-letter topic, as a deployment would.
func testKafkaDLQ(t *testing.T, topic string) {
	t.Helper()
	admin, err := sarama.NewClusterAdmin(testKafkaBrokers, nil)
	if err != nil {
		t.Skipf("Kafka not available at %v, skipping: %v", testKafkaBrokers, err)
	}
	defer admin.Close()
	if err := admin.CreateTopic(KafkaDLQTopic(topic), &sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}, false); err != nil {
		t.Fatalf("create %s: %v", KafkaDLQTopic(topic), err)
	}
}

// readDLQ waits for want messages on topic's dead-letter topic.
func readDLQ(t *testing.T, topic string, want int) []*sarama.ConsumerMessage {
	t.Helper()
	consumer, err := sarama.NewConsumer(testKafkaBrokers, nil)
	if err != nil {
		t.Fatalf("NewConsumer: %v", err)
	}
	defer consumer.Close()
	pc, err := consumer.ConsumePartition(KafkaDLQTopic(topic), 0, sarama.OffsetOldest)
	if err != nil {
		t.Fatalf("ConsumePartition(%s): %v", KafkaDLQTopic(topic), err)
	}
	defer pc.Close()
	var got []*sarama.ConsumerMessage
	deadline := time.After(20 * time.Second)
	for len(got) < want {
		select {
		case m := <-pc.Messages():
			got = append(got, m)
		case <-deadline:
			t.Fatalf("%s holds %d messages after 20s, want %d", KafkaDLQTopic(topic), len(got), want)
		}
	}
	return got
}

func header(m *sarama.ConsumerMessage, key string) string {
	for _, h := range m.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

// startConsumer runs a consumer of topic in group until the test ends, and
// waits until it has joined.
func startConsumer(t *testing.T, topic, group string, dlq sarama.SyncProducer, handler func(context.Context, Event) error) {
	t.Helper()
	consumer, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: group, Topics: []string{topic},
		DLQProducer: dlq, Retry: fastRetry})
	if err != nil {
		t.Fatalf("NewKafkaEventConsumer: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = consumer.Start(ctx, handler)
	}()
	t.Cleanup(func() {
		cancel()
		<-stopped
		_ = consumer.Close()
	})
	if err := consumer.(*kafkaEventConsumer).WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
}

func send(t *testing.T, producer sarama.SyncProducer, topic string, value []byte) {
	t.Helper()
	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Key: sarama.StringEncoder("k"), Value: sarama.ByteEncoder(value)}); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
}

func eventJSON(id string) []byte {
	b, _ := json.Marshal(Event{EventID: id})
	return b
}

// recorder collects the event ids a handler saw, in order.
type recorder struct {
	mu  sync.Mutex
	ids []string
}

func (r *recorder) add(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ids = append(r.ids, id)
}

func (r *recorder) seen() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ids...)
}

func TestKafkaEventConsumer_FailingOnceIsHandledAgain(t *testing.T) {
	topic := testKafkaTopic(t)
	testKafkaDLQ(t, topic)
	producer := rawSaramaProducer(t)

	var tries atomic.Int32
	var rec recorder
	startConsumer(t, topic, "grnoti-test-group-"+topic, producer, func(_ context.Context, e Event) error {
		if e.EventID == "flaky" && tries.Add(1) == 1 {
			return errors.New("not yet")
		}
		rec.add(e.EventID)
		return nil
	})
	send(t, producer, topic, eventJSON("flaky"))
	send(t, producer, topic, eventJSON("after"))

	waitForCondition(t, 15*time.Second, func() bool { return len(rec.seen()) == 2 })
	if got := rec.seen(); got[0] != "flaky" || got[1] != "after" {
		t.Fatalf("handled %v, want [flaky after]: the failed message first, again, and in order", got)
	}
	if tries.Load() != 2 {
		t.Fatalf("flaky was tried %d times, want 2", tries.Load())
	}
}

func TestKafkaEventConsumer_AlwaysFailingIsDeadLetteredThenMarked(t *testing.T) {
	topic := testKafkaTopic(t)
	testKafkaDLQ(t, topic)
	producer := rawSaramaProducer(t)
	group := "grnoti-test-group-" + topic

	var tries atomic.Int32
	var rec recorder
	startConsumer(t, topic, group, producer, func(_ context.Context, e Event) error {
		if e.EventID == "broken" {
			tries.Add(1)
			return errors.New("this one never works")
		}
		rec.add(e.EventID)
		return nil
	})
	send(t, producer, topic, eventJSON("broken"))
	send(t, producer, topic, eventJSON("after"))

	waitForCondition(t, 15*time.Second, func() bool { return len(rec.seen()) == 1 })
	if tries.Load() != int32(fastRetry.Attempts) {
		t.Fatalf("broken was tried %d times, want %d", tries.Load(), fastRetry.Attempts)
	}
	m := readDLQ(t, topic, 1)[0]
	if string(m.Value) != string(eventJSON("broken")) || string(m.Key) != "k" {
		t.Fatalf("dead-lettered %q (key %q), want the message as it came", m.Value, m.Key)
	}
	for key, want := range map[string]string{
		KafkaHeaderError: "this one never works", KafkaHeaderTopic: topic, KafkaHeaderPartition: "0",
		KafkaHeaderOffset: "0", KafkaHeaderAttempts: fmt.Sprint(fastRetry.Attempts),
	} {
		if got := header(m, key); got != want {
			t.Errorf("header %s = %q, want %q", key, got, want)
		}
	}
	assertCommitted(t, topic, group, 2)
}

func TestKafkaEventConsumer_PoisonIsDeadLetteredUntried(t *testing.T) {
	topic := testKafkaTopic(t)
	testKafkaDLQ(t, topic)
	producer := rawSaramaProducer(t)

	var rec recorder
	startConsumer(t, topic, "grnoti-test-group-"+topic, producer, func(_ context.Context, e Event) error {
		rec.add(e.EventID)
		return nil
	})
	send(t, producer, topic, []byte("not-json"))
	send(t, producer, topic, eventJSON("after"))

	waitForCondition(t, 15*time.Second, func() bool { return len(rec.seen()) == 1 })
	m := readDLQ(t, topic, 1)[0]
	if string(m.Value) != "not-json" || header(m, KafkaHeaderAttempts) != "0" {
		t.Fatalf("dead-lettered %q, attempts %q; want the poison message, untried", m.Value, header(m, KafkaHeaderAttempts))
	}
	if got := header(m, KafkaHeaderError); len(got) < 7 || got[:7] != "decode:" {
		t.Fatalf("error header %q, want a decode failure", got)
	}
}

// A dead-letter write that fails stops the partition: nothing after the
// message is handled or committed, and once the DLQ works again (a new
// consumer in the same group) the message is handled from where it stopped.
func TestKafkaEventConsumer_FailedDLQWriteStopsThePartition(t *testing.T) {
	topic := testKafkaTopic(t)
	testKafkaDLQ(t, topic)
	producer := rawSaramaProducer(t)
	group := "grnoti-test-group-" + topic

	send(t, producer, topic, eventJSON("broken"))
	send(t, producer, topic, eventJSON("after"))

	var tries atomic.Int32
	var first recorder
	func() {
		consumer, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: group, Topics: []string{topic},
			DLQProducer: failingSyncProducer{}, Retry: fastRetry})
		if err != nil {
			t.Fatalf("NewKafkaEventConsumer: %v", err)
		}
		defer consumer.Close()
		ctx, cancel := context.WithCancel(context.Background())
		stopped := make(chan struct{})
		go func() {
			defer close(stopped)
			_ = consumer.Start(ctx, func(_ context.Context, e Event) error {
				if e.EventID == "broken" {
					tries.Add(1)
					return errors.New("never")
				}
				first.add(e.EventID)
				return nil
			})
		}()
		// Two halts: the message is tried again after each pause.
		waitForCondition(t, 20*time.Second, func() bool { return tries.Load() >= int32(2*fastRetry.Attempts) })
		cancel()
		<-stopped
	}()
	if got := first.seen(); len(got) != 0 {
		t.Fatalf("handled %v past a message it could not dead-letter, want nothing", got)
	}
	assertCommitted(t, topic, group, -1)

	var second recorder
	startConsumer(t, topic, group, producer, func(_ context.Context, e Event) error {
		if e.EventID == "broken" {
			return errors.New("never")
		}
		second.add(e.EventID)
		return nil
	})
	waitForCondition(t, 15*time.Second, func() bool { return len(second.seen()) == 1 })
	if readDLQ(t, topic, 1)[0].Value == nil {
		t.Fatal("the stopped message never reached the dead-letter topic")
	}
}

// A handler answering ErrClosed (the service shutting down) leaves the
// message unmarked and never dead-letters it.
func TestKafkaEventConsumer_ClosedHandlerLeavesTheMessage(t *testing.T) {
	topic := testKafkaTopic(t)
	testKafkaDLQ(t, topic)
	producer := rawSaramaProducer(t)
	group := "grnoti-test-group-" + topic

	consumer, err := NewKafkaEventConsumer(KafkaConsumerConfig{Brokers: testKafkaBrokers, GroupID: group, Topics: []string{topic},
		DLQProducer: producer, Retry: fastRetry})
	if err != nil {
		t.Fatalf("NewKafkaEventConsumer: %v", err)
	}
	defer consumer.Close()
	send(t, producer, topic, eventJSON("during-shutdown"))

	var tries atomic.Int32
	result := make(chan error, 1)
	go func() {
		result <- consumer.Start(context.Background(), func(context.Context, Event) error {
			tries.Add(1)
			return fmt.Errorf("submit: %w", ErrClosed)
		})
	}()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("Start = %v, want it to stop with ErrClosed", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Start went on after its handler closed")
	}
	if tries.Load() != 1 {
		t.Fatalf("tried %d times, want once: ErrClosed is never retried", tries.Load())
	}
	assertCommitted(t, topic, group, -1)
	assertDLQEmpty(t, topic)
}

// assertCommitted checks group's committed offset on topic's partition 0
// (-1: nothing committed).
func assertCommitted(t *testing.T, topic, group string, want int64) {
	t.Helper()
	// An offset manager reads the committed offset once, when it starts, so
	// each look is a fresh one.
	committed := func() int64 {
		admin, err := sarama.NewClusterAdmin(testKafkaBrokers, nil)
		if err != nil {
			t.Fatalf("NewClusterAdmin: %v", err)
		}
		defer admin.Close()
		resp, err := admin.ListConsumerGroupOffsets(group, map[string][]int32{topic: {0}})
		if err != nil {
			t.Fatalf("ListConsumerGroupOffsets: %v", err)
		}
		if b := resp.GetBlock(topic, 0); b != nil {
			return b.Offset
		}
		return -1
	}
	var got int64
	deadline := time.Now().Add(10 * time.Second)
	for {
		got = committed()
		if got == want || time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if got != want {
		t.Fatalf("group %s committed %d on %s, want %d", group, got, topic, want)
	}
}

func assertDLQEmpty(t *testing.T, topic string) {
	t.Helper()
	client, err := sarama.NewClient(testKafkaBrokers, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	newest, err := client.GetOffset(KafkaDLQTopic(topic), 0, sarama.OffsetNewest)
	if err != nil {
		t.Fatalf("GetOffset: %v", err)
	}
	if newest != 0 {
		t.Fatalf("%s holds %d messages, want none", KafkaDLQTopic(topic), newest)
	}
}
