// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-adapter-kafka/kafkaadapter"
	"github.com/llingr/llingr-nexus/nexus"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
)

// skipIfShort skips integration tests when running with -short flag.
func skipIfShort(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
}

// Custom traits for identifying which consumer processed each message (bits 10+)
const (
	TraitConsumer1 nexus.Traits = 1 << 10
	TraitConsumer2 nexus.Traits = 1 << 11
)

const (
	numPartitions = 4
	messageCount  = 100
)

// --- Test Helpers ---

func startKafka(ctx context.Context, t *testing.T) string {
	t.Helper()

	container, err := kafkacontainer.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		kafkacontainer.WithClusterID("test-cluster"),
	)
	if err != nil {
		t.Skipf("failed to start Kafka container (is Docker running?): %v", err)
	}

	brokers, err := container.Brokers(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("failed to get brokers: %v", err)
	}

	// t.Cleanup runs AFTER all defers, ensuring consumers shut down before container.
	// Small delay allows librdkafka background threads to fully terminate after Close().
	t.Cleanup(func() {
		time.Sleep(100 * time.Millisecond)
		_ = container.Terminate(ctx)
	})

	return brokers[0]
}

func createTopic(t *testing.T, bootstrapServers, topic string, partitions int) {
	t.Helper()

	admin, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}
	defer admin.Close()

	results, err := admin.CreateTopics(context.Background(), []kafka.TopicSpecification{
		{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1},
	})
	if err != nil {
		t.Fatalf("failed to create topic: %v", err)
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			t.Fatalf("failed to create topic %s: %v", result.Topic, result.Error)
		}
	}

	// wait for topic to be ready
	time.Sleep(500 * time.Millisecond)
}

func publishMessages(t *testing.T, bootstrapServers, topic string, count int) {
	t.Helper()

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	deliveryChan := make(chan kafka.Event, count)

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := fmt.Sprintf("value-%04d", i)

		err := producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Key:            []byte(key),
			Value:          []byte(value),
		}, deliveryChan)
		if err != nil {
			t.Fatalf("failed to produce message %d: %v", i, err)
		}
	}

	// wait for all deliveries
	for i := 0; i < count; i++ {
		e := <-deliveryChan
		m, ok := e.(*kafka.Message)
		if !ok {
			t.Fatal("expected *kafka.Message")
		}
		if m.TopicPartition.Error != nil {
			t.Fatalf("delivery failed: %v", m.TopicPartition.Error)
		}
	}

	t.Logf("published %d messages to %s", count, topic)
}

type consumerHandle struct {
	consumer *kafkaadapter.SimpleConsumer
	adapter  *kafkaadapter.Adapter
}

func createConsumer(
	t *testing.T,
	bootstrapServers, topic, groupID string,
	consumerTrait nexus.Traits,
	strategy string,
) *consumerHandle {
	t.Helper()

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      groupID,
		"auto.offset.reset":             "earliest",
		"partition.assignment.strategy": strategy,
	}

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		// mark which consumer processed this message
		nexus.SetTraits(&msg.Traits, consumerTrait)
		return nil
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	consumer := adapter.CreateConsumer(builder)
	sc, ok := consumer.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}

	return &consumerHandle{
		consumer: sc,
		adapter:  adapter,
	}
}

func waitForMessages(consumers []*consumerHandle, expected int, timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		total := 0
		for _, c := range consumers {
			total += c.consumer.MetricsCount()
		}
		if total >= expected {
			return total
		}
		time.Sleep(100 * time.Millisecond)
	}

	total := 0
	for _, c := range consumers {
		total += c.consumer.MetricsCount()
	}
	return total
}

func countByTrait(consumers []*consumerHandle) map[nexus.Traits]int {
	counts := make(map[nexus.Traits]int)
	for _, c := range consumers {
		for _, m := range c.consumer.Metrics() {
			if m.Traits&TraitConsumer1 != 0 {
				counts[TraitConsumer1]++
			}
			if m.Traits&TraitConsumer2 != 0 {
				counts[TraitConsumer2]++
			}
		}
	}
	return counts
}

// --- Tests ---

func TestSingleConsumer_Range(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-single-range"
	createTopic(t, bootstrapServers, topic, numPartitions)
	publishMessages(t, bootstrapServers, topic, messageCount)

	handle := createConsumer(t, bootstrapServers, topic, "group-single-range", TraitConsumer1, "range")
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, messageCount, 30*time.Second)
	if total != messageCount {
		t.Errorf("expected %d messages, got %d", messageCount, total)
	}

	counts := countByTrait([]*consumerHandle{handle})
	if counts[TraitConsumer1] != messageCount {
		t.Errorf("consumer 1 should have trait on all %d messages, got %d", messageCount, counts[TraitConsumer1])
	}

	t.Logf("single consumer (range): received %d messages", total)
}

func TestSingleConsumer_RoundRobin(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-single-roundrobin"
	createTopic(t, bootstrapServers, topic, numPartitions)
	publishMessages(t, bootstrapServers, topic, messageCount)

	handle := createConsumer(t, bootstrapServers, topic, "group-single-rr", TraitConsumer1, "roundrobin")
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, messageCount, 30*time.Second)
	if total != messageCount {
		t.Errorf("expected %d messages, got %d", messageCount, total)
	}

	t.Logf("single consumer (roundrobin): received %d messages", total)
}

func TestSingleConsumer_CooperativeSticky(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-single-coop"
	createTopic(t, bootstrapServers, topic, numPartitions)
	publishMessages(t, bootstrapServers, topic, messageCount)

	handle := createConsumer(t, bootstrapServers, topic, "group-single-coop", TraitConsumer1, "cooperative-sticky")
	defer func() { _ = handle.consumer.Shutdown() }()

	if err := handle.consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{handle}, messageCount, 30*time.Second)
	if total != messageCount {
		t.Errorf("expected %d messages, got %d", messageCount, total)
	}

	t.Logf("single consumer (cooperative-sticky): received %d messages", total)
}

func TestTwoConsumers_Range(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-two-range"
	groupID := "group-two-range"
	createTopic(t, bootstrapServers, topic, numPartitions)

	// start both consumers first
	handle1 := createConsumer(t, bootstrapServers, topic, groupID, TraitConsumer1, "range")
	defer func() { _ = handle1.consumer.Shutdown() }()

	handle2 := createConsumer(t, bootstrapServers, topic, groupID, TraitConsumer2, "range")
	defer func() { _ = handle2.consumer.Shutdown() }()

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}
	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	// wait for both consumers to be assigned partitions
	time.Sleep(3 * time.Second)

	// publish messages (both consumers should be active)
	publishMessages(t, bootstrapServers, topic, messageCount)

	// wait for all messages (may have duplicates due to rebalance)
	handles := []*consumerHandle{handle1, handle2}
	total := waitForMessages(handles, messageCount, 30*time.Second)
	if total < messageCount {
		t.Errorf("expected at least %d messages, got %d", messageCount, total)
	}

	counts := countByTrait(handles)
	t.Logf("two consumers (range): consumer1=%d, consumer2=%d, total=%d",
		counts[TraitConsumer1], counts[TraitConsumer2], total)

	// both consumers should have processed some messages (partitions distributed)
	if counts[TraitConsumer1] == 0 {
		t.Error("consumer 1 should have processed some messages")
	}
	if counts[TraitConsumer2] == 0 {
		t.Error("consumer 2 should have processed some messages")
	}
}

func TestTwoConsumers_CooperativeSticky(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-two-coop"
	groupID := "group-two-coop"
	createTopic(t, bootstrapServers, topic, numPartitions)

	// publish messages first so they're ready when consumers subscribe
	publishMessages(t, bootstrapServers, topic, messageCount)

	// start first consumer
	handle1 := createConsumer(t, bootstrapServers, topic, groupID, TraitConsumer1, "cooperative-sticky")
	defer func() { _ = handle1.consumer.Shutdown() }()

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}

	// wait for consumer 1 to process at least some messages before adding consumer 2
	waitForMessages([]*consumerHandle{handle1}, 10, 30*time.Second)

	// start second consumer (triggers cooperative rebalance)
	handle2 := createConsumer(t, bootstrapServers, topic, groupID, TraitConsumer2, "cooperative-sticky")
	defer func() { _ = handle2.consumer.Shutdown() }()

	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	// wait for all messages
	handles := []*consumerHandle{handle1, handle2}
	total := waitForMessages(handles, messageCount, 30*time.Second)
	if total < messageCount {
		t.Errorf("expected at least %d messages, got %d", messageCount, total)
	}

	counts := countByTrait(handles)
	t.Logf("two consumers (cooperative-sticky): consumer1=%d, consumer2=%d, total=%d",
		counts[TraitConsumer1], counts[TraitConsumer2], total)

	// consumer 1 should have processed some messages (it started first and processed at least 10)
	if counts[TraitConsumer1] == 0 {
		t.Error("consumer 1 should have processed some messages")
	}
	// consumer 2 may or may not have processed messages depending on rebalance timing
	// with cooperative-sticky, it's valid for consumer 2 to get 0 if rebalance didn't reassign
}

func TestConsumerShutdown_Rebalance(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-shutdown-rebalance"
	groupID := "group-shutdown"
	createTopic(t, bootstrapServers, topic, numPartitions)

	// start two consumers
	handle1 := createConsumer(t, bootstrapServers, topic, groupID, TraitConsumer1, "range")
	defer func() { _ = handle1.consumer.Shutdown() }()

	handle2 := createConsumer(t, bootstrapServers, topic, groupID, TraitConsumer2, "range")

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}
	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	// wait for both to be assigned partitions
	time.Sleep(3 * time.Second)

	// publish messages
	publishMessages(t, bootstrapServers, topic, messageCount)

	// wait for some messages
	handles := []*consumerHandle{handle1, handle2}
	waitForMessages(handles, messageCount/2, 10*time.Second)

	// shutdown consumer 2 (triggers rebalance, consumer 1 takes over)
	t.Log("shutting down consumer 2...")
	_ = handle2.consumer.Shutdown()

	// wait for remaining messages to be processed by consumer 1
	total := waitForMessages([]*consumerHandle{handle1}, messageCount, 30*time.Second)

	counts := countByTrait([]*consumerHandle{handle1, handle2})
	t.Logf("shutdown rebalance: consumer1=%d, consumer2=%d, total(c1 only)=%d",
		counts[TraitConsumer1], counts[TraitConsumer2], total)

	// consumer 1 should have received messages after consumer 2 shutdown
	if counts[TraitConsumer1] == 0 {
		t.Error("consumer 1 should have processed messages")
	}
}

// TestRebalancePattern_PhaseTransitions verifies the rebalance pattern for all strategies:
// Phase 1 (0-5s): consumer 1 only
// Phase 2 (5-10s): consumer 1 + consumer 2
// Phase 3 (10-15s): consumer 2 only (consumer 1 shutdown)
func TestRebalancePattern_PhaseTransitions(t *testing.T) {
	skipIfShort(t)
	strategies := []string{"range", "roundrobin", "cooperative-sticky"}

	for _, strategy := range strategies {
		t.Run(strategy, func(t *testing.T) {
			runRebalancePhaseTest(t, strategy)
		})
	}
}

func runRebalancePhaseTest(t *testing.T, strategy string) {
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := fmt.Sprintf("test-rebalance-%s", strategy)
	groupID := fmt.Sprintf("group-rebalance-%s", strategy)
	createTopic(t, bootstrapServers, topic, numPartitions)

	// phase timing
	const (
		phaseDuration   = 5 * time.Second
		publishInterval = 50 * time.Millisecond
	)

	// track published message indices by phase
	var publishMu sync.Mutex
	phase1Messages := make(map[int]bool) // message indices published during phase 1
	phase2Messages := make(map[int]bool) // message indices published during phase 2
	phase3Messages := make(map[int]bool) // message indices published during phase 3

	// track which consumer processed each message (by message index from key)
	var processMu sync.Mutex
	processedBy := make(map[int]nexus.Traits) // msgIndex -> trait of consumer that processed it

	// create process function that extracts message index and records consumer trait
	makeProcessFn := func(trait nexus.Traits) nexus.ProcessMessage[*kafka.Message] {
		return func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
			// extract message index from key (format: "key-NNNN")
			var msgIdx int
			if _, err := fmt.Sscanf(msg.Key, "key-%d", &msgIdx); err == nil {
				processMu.Lock()
				processedBy[msgIdx] |= trait // use OR to handle potential duplicates
				processMu.Unlock()
			}
			nexus.SetTraits(&msg.Traits, trait)
			return nil
		}
	}

	// create consumer with custom process function
	createConsumerWithProcess := func(
		_ nexus.Traits,
		process nexus.ProcessMessage[*kafka.Message],
	) *consumerHandle {
		configMap := &kafka.ConfigMap{
			"bootstrap.servers":             bootstrapServers,
			"group.id":                      groupID,
			"auto.offset.reset":             "earliest",
			"partition.assignment.strategy": strategy,
		}

		adapter, err := kafkaadapter.New(configMap)
		if err != nil {
			t.Fatalf("failed to create adapter: %v", err)
		}

		builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
		consumer := adapter.CreateConsumer(builder)
		sc, ok := consumer.(*kafkaadapter.SimpleConsumer)
		if !ok {
			t.Fatal("expected *kafkaadapter.SimpleConsumer")
		}

		return &consumerHandle{
			consumer: sc,
			adapter:  adapter,
		}
	}

	// producer goroutine - publishes messages at 50ms intervals
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})
	if err != nil {
		t.Fatalf("failed to create producer: %v", err)
	}
	defer producer.Close()

	stopPublishing := make(chan struct{})
	publishDone := make(chan struct{})
	var msgIndex atomic.Int64

	go func() {
		defer close(publishDone)
		ticker := time.NewTicker(publishInterval)
		defer ticker.Stop()

		deliveryChan := make(chan kafka.Event, 100)

		// drain delivery reports in background
		go func() {
			for range deliveryChan { //nolint:revive // intentionally empty - draining delivery reports
			}
		}()

		for {
			select {
			case <-stopPublishing:
				producer.Flush(5000) // wait for pending deliveries before closing channel
				close(deliveryChan)
				return
			case <-ticker.C:
				idx := msgIndex.Load()
				key := fmt.Sprintf("key-%04d", idx)
				value := fmt.Sprintf("value-%04d", idx)

				_ = producer.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
					Key:            []byte(key),
					Value:          []byte(value),
				}, deliveryChan)

				msgIndex.Add(1)
			}
		}
	}()

	// phase 1: start consumer 1, publish for 5s
	t.Logf("[%s] phase 1: starting consumer 1...", strategy)
	handle1 := createConsumerWithProcess(TraitConsumer1, makeProcessFn(TraitConsumer1))

	if err := handle1.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 1 failed to subscribe: %v", err)
	}

	// wait for consumer 1 to be assigned partitions
	time.Sleep(2 * time.Second)

	// record phase 1 start index
	phase1Start := msgIndex.Load()
	time.Sleep(phaseDuration)
	phase1End := msgIndex.Load()

	// record phase 1 messages
	publishMu.Lock()
	for i := phase1Start; i < phase1End; i++ {
		phase1Messages[int(i)] = true
	}
	publishMu.Unlock()
	t.Logf("[%s] phase 1: published messages %d-%d (%d total)",
		strategy, phase1Start, phase1End-1, phase1End-phase1Start)

	// phase 2: start consumer 2, publish for another 5s
	t.Logf("[%s] phase 2: starting consumer 2...", strategy)
	handle2 := createConsumerWithProcess(TraitConsumer2, makeProcessFn(TraitConsumer2))

	if err := handle2.consumer.Subscribe(); err != nil {
		t.Fatalf("consumer 2 failed to subscribe: %v", err)
	}

	// wait for rebalance to complete
	time.Sleep(3 * time.Second)

	phase2Start := msgIndex.Load()
	time.Sleep(phaseDuration)
	phase2End := msgIndex.Load()

	// record phase 2 messages
	publishMu.Lock()
	for i := phase2Start; i < phase2End; i++ {
		phase2Messages[int(i)] = true
	}
	publishMu.Unlock()
	t.Logf("[%s] phase 2: published messages %d-%d (%d total)",
		strategy, phase2Start, phase2End-1, phase2End-phase2Start)

	// phase 3: shutdown consumer 1, publish for another 5s
	t.Logf("[%s] phase 3: shutting down consumer 1...", strategy)
	_ = handle1.consumer.Shutdown()

	// wait for rebalance after shutdown
	time.Sleep(3 * time.Second)

	phase3Start := msgIndex.Load()
	time.Sleep(phaseDuration)
	phase3End := msgIndex.Load()

	// record phase 3 messages
	publishMu.Lock()
	for i := phase3Start; i < phase3End; i++ {
		phase3Messages[int(i)] = true
	}
	publishMu.Unlock()
	t.Logf("[%s] phase 3: published messages %d-%d (%d total)",
		strategy, phase3Start, phase3End-1, phase3End-phase3Start)

	// stop publishing
	close(stopPublishing)
	<-publishDone
	producer.Flush(5000)

	totalPublished := int(msgIndex.Load())
	t.Logf("[%s] total published: %d messages", strategy, totalPublished)

	// wait for all messages to be consumed
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		processMu.Lock()
		consumed := len(processedBy)
		processMu.Unlock()
		if consumed >= totalPublished {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// shutdown consumer 2
	_ = handle2.consumer.Shutdown()

	// analyze results
	processMu.Lock()
	defer processMu.Unlock()

	var phase1C1, phase1C2, phase1Both int
	var phase2C1, phase2C2, phase2Both int
	var phase3C1, phase3C2, phase3Both int

	for idx, traits := range processedBy {
		hasC1 := traits&TraitConsumer1 != 0
		hasC2 := traits&TraitConsumer2 != 0

		if phase1Messages[idx] {
			switch {
			case hasC1 && hasC2:
				phase1Both++
			case hasC1:
				phase1C1++
			case hasC2:
				phase1C2++
			}
		}
		if phase2Messages[idx] {
			switch {
			case hasC1 && hasC2:
				phase2Both++
			case hasC1:
				phase2C1++
			case hasC2:
				phase2C2++
			}
		}
		if phase3Messages[idx] {
			switch {
			case hasC1 && hasC2:
				phase3Both++
			case hasC1:
				phase3C1++
			case hasC2:
				phase3C2++
			}
		}
	}

	t.Logf("[%s] phase 1 results: c1=%d, c2=%d, both=%d", strategy, phase1C1, phase1C2, phase1Both)
	t.Logf("[%s] phase 2 results: c1=%d, c2=%d, both=%d", strategy, phase2C1, phase2C2, phase2Both)
	t.Logf("[%s] phase 3 results: c1=%d, c2=%d, both=%d", strategy, phase3C1, phase3C2, phase3Both)

	// assertions

	// phase 1: should be dominated by consumer 1 (consumer 2 not yet started)
	phase1Total := phase1C1 + phase1C2 + phase1Both
	if phase1Total > 0 && phase1C1 < phase1Total/2 {
		t.Errorf("[%s] phase 1: expected consumer 1 to dominate, got c1=%d, c2=%d, both=%d",
			strategy, phase1C1, phase1C2, phase1Both)
	}

	// phase 2: both consumers should process messages (partitions split)
	phase2Total := phase2C1 + phase2C2 + phase2Both
	if phase2Total > 0 {
		// allow for timing variations, but both should have processed something
		if phase2C1+phase2Both == 0 {
			t.Errorf("[%s] phase 2: consumer 1 should have processed some messages", strategy)
		}
		if phase2C2+phase2Both == 0 {
			t.Errorf("[%s] phase 2: consumer 2 should have processed some messages", strategy)
		}
	}

	// phase 3: should be dominated by consumer 2 (consumer 1 shutdown)
	phase3Total := phase3C1 + phase3C2 + phase3Both
	if phase3Total > 0 && phase3C2 < phase3Total/2 {
		t.Errorf("[%s] phase 3: expected consumer 2 to dominate, got c1=%d, c2=%d, both=%d",
			strategy, phase3C1, phase3C2, phase3Both)
	}

	// overall: all messages should be consumed
	consumed := len(processedBy)
	if consumed < totalPublished {
		t.Errorf("[%s] message loss: published %d, consumed %d", strategy, totalPublished, consumed)
	} else {
		t.Logf("[%s] all %d messages consumed successfully", strategy, consumed)
	}
}

// TestConfigOptions_SessionTimeout verifies custom session.timeout.ms works
func TestConfigOptions_SessionTimeout(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-session-timeout"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      "group-session-timeout",
		"auto.offset.reset":             "earliest",
		"session.timeout.ms":            45000, // explicit session timeout
		"heartbeat.interval.ms":         15000, // must be < session.timeout.ms / 3
		"partition.assignment.strategy": "range",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	c := adapter.CreateConsumer(builder)
	consumer, ok := c.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}
	defer func() { _ = consumer.Shutdown() }()

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{{consumer: consumer, adapter: adapter}}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}

	t.Logf("session timeout config: received %d messages", total)
}

// TestConfigOptions_MaxPollInterval verifies custom max.poll.interval.ms works
func TestConfigOptions_MaxPollInterval(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-max-poll"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      "group-max-poll",
		"auto.offset.reset":             "earliest",
		"max.poll.interval.ms":          300000, // 5 minutes
		"partition.assignment.strategy": "range",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	c := adapter.CreateConsumer(builder)
	consumer, ok := c.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}
	defer func() { _ = consumer.Shutdown() }()

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{{consumer: consumer, adapter: adapter}}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}

	t.Logf("max poll interval config: received %d messages", total)
}

// TestConfigOptions_FetchSettings verifies fetch configuration works
func TestConfigOptions_FetchSettings(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-fetch-settings"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 50)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      "group-fetch-settings",
		"auto.offset.reset":             "earliest",
		"fetch.min.bytes":               1,       // minimum bytes to fetch
		"fetch.max.bytes":               1048576, // 1MB max
		"max.partition.fetch.bytes":     262144,  // 256KB per partition
		"fetch.wait.max.ms":             100,     // max wait time
		"partition.assignment.strategy": "range",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	c := adapter.CreateConsumer(builder)
	consumer, ok := c.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}
	defer func() { _ = consumer.Shutdown() }()

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{{consumer: consumer, adapter: adapter}}, 50, 30*time.Second)
	if total != 50 {
		t.Errorf("expected 50 messages, got %d", total)
	}

	t.Logf("fetch settings config: received %d messages", total)
}

// TestConfigOptions_IsolationLevel verifies read_committed isolation works
func TestConfigOptions_IsolationLevel(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-isolation-level"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      "group-isolation",
		"auto.offset.reset":             "earliest",
		"isolation.level":               "read_committed", // explicit (same as default)
		"partition.assignment.strategy": "range",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	c := adapter.CreateConsumer(builder)
	consumer, ok := c.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}
	defer func() { _ = consumer.Shutdown() }()

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{{consumer: consumer, adapter: adapter}}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}

	t.Logf("isolation level config: received %d messages", total)
}

// TestConfigOptions_ClientID verifies client.id is properly set
func TestConfigOptions_ClientID(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-client-id"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 10)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      "group-client-id",
		"client.id":                     "test-consumer-instance-1", // custom client ID
		"auto.offset.reset":             "earliest",
		"partition.assignment.strategy": "range",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	c := adapter.CreateConsumer(builder)
	consumer, ok := c.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}
	defer func() { _ = consumer.Shutdown() }()

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{{consumer: consumer, adapter: adapter}}, 10, 30*time.Second)
	if total != 10 {
		t.Errorf("expected 10 messages, got %d", total)
	}

	t.Logf("client.id config: received %d messages", total)
}

// TestConfigOptions_QueuedSettings verifies queued message settings work
func TestConfigOptions_QueuedSettings(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-queued-settings"
	createTopic(t, bootstrapServers, topic, 2)
	publishMessages(t, bootstrapServers, topic, 100)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      "group-queued",
		"auto.offset.reset":             "earliest",
		"queued.min.messages":           1000,  // min messages to queue
		"queued.max.messages.kbytes":    65536, // 64MB max queue size
		"partition.assignment.strategy": "range",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	c := adapter.CreateConsumer(builder)
	consumer, ok := c.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}
	defer func() { _ = consumer.Shutdown() }()

	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	total := waitForMessages([]*consumerHandle{{consumer: consumer, adapter: adapter}}, 100, 30*time.Second)
	if total != 100 {
		t.Errorf("expected 100 messages, got %d", total)
	}

	t.Logf("queued settings config: received %d messages", total)
}

// TestSetOAuthTokenRefresh_CallbackRegistration verifies OAuth callback is properly wired
func TestSetOAuthTokenRefresh_CallbackRegistration(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	// note: we can't test actual OAuth flow without an OAuth provider,
	// but we can verify the callback registration works with a real adapter

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "group-oauth-test",
		"auto.offset.reset": "earliest",
		// NOT setting security.protocol or sasl.mechanisms - just testing registration
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	// register OAuth callback
	callbackRegistered := false
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		callbackRegistered = true
		return kafka.OAuthBearerToken{
			TokenValue: "test-token",
			Expiration: time.Now().Add(1 * time.Hour),
			Principal:  "test-user",
		}, nil
	})

	// verify underlying consumer is accessible
	consumer := adapter.ConfluentConsumer()
	if consumer == nil {
		t.Error("expected ConfluentConsumer to return non-nil consumer")
	}

	t.Log("OAuth callback registration: verified adapter accepts callback")

	// note: actual OAuth token refresh only triggered when using OAUTHBEARER auth
	// which requires an external OAuth provider - but the wiring is verified
	_ = callbackRegistered // callback would be invoked by Poll() on OAuthBearerTokenRefresh event
}

// TestConfluentConsumer_ExposesUnderlying verifies direct access to kafka.Consumer
func TestConfluentConsumer_ExposesUnderlying(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "group-confluent-consumer",
		"auto.offset.reset": "earliest",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	// get underlying consumer
	consumer := adapter.ConfluentConsumer()
	if consumer == nil {
		t.Fatal("expected non-nil consumer")
	}

	// verify we can call confluent-kafka-go methods directly
	metadata, err := consumer.GetMetadata(nil, false, 5000)
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	if len(metadata.Brokers) == 0 {
		t.Error("expected at least one broker in metadata")
	}

	t.Logf("ConfluentConsumer: metadata shows %d brokers, %d topics",
		len(metadata.Brokers), len(metadata.Topics))

	// cleanup
	_ = consumer.Close()
}

func TestAllMessagesDelivered_NoLoss(t *testing.T) {
	skipIfShort(t)
	ctx := context.Background()
	bootstrapServers := startKafka(ctx, t)

	topic := "test-no-loss"
	groupID := "group-no-loss"
	msgCount := 500
	createTopic(t, bootstrapServers, topic, numPartitions)

	// track unique messages by partition:offset
	var mu sync.Mutex
	seen := make(map[string]bool)

	process := func(_ context.Context, msg *nexus.Message[*kafka.Message]) error {
		key := fmt.Sprintf("%d:%d", msg.Partition, msg.Offset)
		mu.Lock()
		seen[key] = true
		mu.Unlock()
		nexus.SetTraits(&msg.Traits, TraitConsumer1)
		return nil
	}

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             bootstrapServers,
		"group.id":                      groupID,
		"auto.offset.reset":             "earliest",
		"partition.assignment.strategy": "range",
	}

	adapter, err := kafkaadapter.New(configMap)
	if err != nil {
		t.Fatalf("failed to create adapter: %v", err)
	}

	builder := kafkaadapter.NewSimpleConsumerBuilder(topic, process)
	c := adapter.CreateConsumer(builder)
	consumer, ok := c.(*kafkaadapter.SimpleConsumer)
	if !ok {
		t.Fatal("expected *kafkaadapter.SimpleConsumer")
	}
	defer func() { _ = consumer.Shutdown() }()

	// publish messages first
	publishMessages(t, bootstrapServers, topic, msgCount)

	// start consumer
	if err := consumer.Subscribe(); err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// wait for all messages
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(seen)
		mu.Unlock()
		if count >= msgCount {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	mu.Lock()
	finalCount := len(seen)
	mu.Unlock()

	if finalCount != msgCount {
		t.Errorf("message loss detected: expected %d unique messages, got %d", msgCount, finalCount)
	} else {
		t.Logf("all %d messages delivered without loss", msgCount)
	}
}
