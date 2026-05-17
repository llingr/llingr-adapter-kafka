// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

// sampleLibrdkafkaStats is a minimal but representative librdkafka stats JSON blob.
const sampleLibrdkafkaStats = `{
  "brokers": {
    "broker-1:9092/1": {
      "nodeid": 1,
      "name": "broker-1:9092/1",
      "host": "broker-1",
      "port": 9092,
      "rack": "us-east-1a",
      "txbytes": 1024,
      "rxbytes": 2048
    },
    "broker-2:9092/2": {
      "nodeid": 2,
      "name": "broker-2:9092/2",
      "host": "broker-2",
      "port": 9093,
      "rack": "us-east-1b",
      "txbytes": 512,
      "rxbytes": 4096
    },
    "bootstrap:9092/-1": {
      "nodeid": -1,
      "name": "bootstrap:9092/-1",
      "host": "bootstrap",
      "port": 9092,
      "rack": "",
      "txbytes": 100,
      "rxbytes": 200
    }
  },
  "topics": {
    "orders": {
      "partitions": {
        "0": {"partition": 0, "leader": 1, "rxbytes": 10000, "txbytes": 500, "rxmsgs": 100},
        "1": {"partition": 1, "leader": 2, "rxbytes": 20000, "txbytes": 1000, "rxmsgs": 200},
        "2": {"partition": 2, "leader": 1, "rxbytes": 15000, "txbytes": 750, "rxmsgs": 150},
        "-1": {"partition": -1, "leader": -1, "rxbytes": 45000, "txbytes": 2250, "rxmsgs": 450}
      }
    }
  }
}`

func TestBandwidthCollector_HandleStats(t *testing.T) {
	var received nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback: func(m nexus.BandwidthMetrics) {
			received = m
		},
		interval:  time.Minute,
		groupID:   "test-group",
		topicName: "orders",
	}

	bc.handleStats(sampleLibrdkafkaStats)

	if received.BandwidthMetricsID == "" {
		t.Fatal("expected non-empty BandwidthMetricsID")
	}
	if received.TopicName != "orders" {
		t.Errorf("expected TopicName 'orders', got %q", received.TopicName)
	}
	if received.ConsumerGroup != "test-group" {
		t.Errorf("expected ConsumerGroup 'test-group', got %q", received.ConsumerGroup)
	}
	if received.StatsIntervalDuration != time.Minute {
		t.Errorf("expected interval 1m, got %v", received.StatsIntervalDuration)
	}
}

func TestBandwidthCollector_BrokerMetadata(t *testing.T) {
	var received nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { received = m },
		interval:  time.Minute,
		topicName: "orders",
	}

	bc.handleStats(sampleLibrdkafkaStats)

	// should exclude bootstrap broker (nodeid -1)
	if len(received.Brokers) != 2 {
		t.Fatalf("expected 2 brokers (excluding bootstrap), got %d", len(received.Brokers))
	}

	// verify broker data (order not guaranteed from map iteration)
	brokersByID := make(map[string]nexus.BrokerInfo)
	for _, b := range received.Brokers {
		brokersByID[b.ID] = b
	}

	b1 := brokersByID["1"]
	if b1.Host != "broker-1" {
		t.Errorf("broker 1: expected Host 'broker-1', got %q", b1.Host)
	}
	if b1.Port != "9092" {
		t.Errorf("broker 1: expected Port '9092', got %q", b1.Port)
	}
	if b1.Rack != "us-east-1a" {
		t.Errorf("broker 1: expected Rack 'us-east-1a', got %q", b1.Rack)
	}

	b2 := brokersByID["2"]
	if b2.Host != "broker-2" {
		t.Errorf("broker 2: expected Host 'broker-2', got %q", b2.Host)
	}
	if b2.Rack != "us-east-1b" {
		t.Errorf("broker 2: expected Rack 'us-east-1b', got %q", b2.Rack)
	}
}

func TestBandwidthCollector_CompressionFieldsZero(t *testing.T) {
	var received nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { received = m },
		interval:  time.Minute,
		topicName: "orders",
	}

	bc.handleStats(sampleLibrdkafkaStats)

	for _, p := range received.Partitions {
		if p.CompressedBytes != 0 {
			t.Errorf("partition %d: CompressedBytes should be zero (kafka adapter), got %d", p.ID, p.CompressedBytes)
		}
		if p.UncompressedBytes != 0 {
			t.Errorf("partition %d: UncompressedBytes should be zero, got %d", p.ID, p.UncompressedBytes)
		}
		if p.Compression != "" {
			t.Errorf("partition %d: Compression should be empty, got %q", p.ID, p.Compression)
		}
	}
}

func TestBandwidthCollector_UnknownTopic(t *testing.T) {
	var received nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { received = m },
		interval:  time.Minute,
		topicName: "nonexistent-topic",
	}

	bc.handleStats(sampleLibrdkafkaStats)

	if len(received.Partitions) != 0 {
		t.Errorf("expected 0 partitions for unknown topic, got %d", len(received.Partitions))
	}
}

func TestBandwidthCollector_MalformedJSON(t *testing.T) {
	called := false
	bc := &bandwidthCollector{
		callback:  func(_ nexus.BandwidthMetrics) { called = true },
		interval:  time.Minute,
		topicName: "orders",
	}

	bc.handleStats("{invalid json")

	if called {
		t.Error("callback should not be called on malformed JSON")
	}
}

func TestBandwidthCollector_NilCallback(t *testing.T) {
	bc := &bandwidthCollector{
		callback:  nil,
		interval:  time.Minute,
		topicName: "orders",
	}

	// should not panic
	bc.handleStats(sampleLibrdkafkaStats)
}

// --- Delta computation tests ---
//
// librdkafka emits cumulative counters. The collector must compute
// deltas (current - previous) and emit those. These tests verify
// byte-perfect accuracy across multiple stats intervals.

func TestBandwidthCollector_FirstCallEmitsCumulativeAsDelta(t *testing.T) {
	// First stats event has no previous snapshot, so the cumulative
	// values themselves ARE the delta (delta from zero).
	var received nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { received = m },
		interval:  time.Minute,
		topicName: "orders",
	}

	bc.handleStats(sampleLibrdkafkaStats)

	partsByID := partitionMap(received.Partitions)

	// partition 0: rxbytes=10000, txbytes=500, rxmsgs=100
	assertPartitionExact(t, partsByID[0], 10000, 500, 100, "first call, partition 0")
	// partition 1: rxbytes=20000, txbytes=1000, rxmsgs=200
	assertPartitionExact(t, partsByID[1], 20000, 1000, 200, "first call, partition 1")
	// partition 2: rxbytes=15000, txbytes=750, rxmsgs=150
	assertPartitionExact(t, partsByID[2], 15000, 750, 150, "first call, partition 2")
}

func TestBandwidthCollector_SecondCallEmitsDelta(t *testing.T) {
	var received nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { received = m },
		interval:  time.Minute,
		topicName: "orders",
	}

	// first call seeds the previous snapshot
	bc.handleStats(sampleLibrdkafkaStats)

	// second call with higher cumulative values
	bc.handleStats(makeStats(30000, 1500, 300, 50000, 2500, 500, 25000, 1250, 250))

	partsByID := partitionMap(received.Partitions)

	// delta = second - first
	// partition 0: 30000-10000=20000 rx, 1500-500=1000 tx, 300-100=200 msgs
	assertPartitionExact(t, partsByID[0], 20000, 1000, 200, "second call, partition 0")
	// partition 1: 50000-20000=30000 rx, 2500-1000=1500 tx, 500-200=300 msgs
	assertPartitionExact(t, partsByID[1], 30000, 1500, 300, "second call, partition 1")
	// partition 2: 25000-15000=10000 rx, 1250-750=500 tx, 250-150=100 msgs
	assertPartitionExact(t, partsByID[2], 10000, 500, 100, "second call, partition 2")
}

func TestBandwidthCollector_ThreeConsecutiveIntervals(t *testing.T) {
	var packets []nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { packets = append(packets, m) },
		interval:  time.Second,
		topicName: "orders",
	}

	// interval 1: cumulative 1000, 100, 10
	bc.handleStats(makeStats(1000, 100, 10, 2000, 200, 20, 3000, 300, 30))
	// interval 2: cumulative 3500, 350, 35
	bc.handleStats(makeStats(3500, 350, 35, 7000, 700, 70, 4500, 450, 45))
	// interval 3: cumulative 8000, 800, 80
	bc.handleStats(makeStats(8000, 800, 80, 9000, 900, 90, 10000, 1000, 100))

	if len(packets) != 3 {
		t.Fatalf("expected 3 packets, got %d", len(packets))
	}

	// interval 1: delta from zero = cumulative itself
	p1 := partitionMap(packets[0].Partitions)
	assertPartitionExact(t, p1[0], 1000, 100, 10, "interval 1, partition 0")
	assertPartitionExact(t, p1[1], 2000, 200, 20, "interval 1, partition 1")
	assertPartitionExact(t, p1[2], 3000, 300, 30, "interval 1, partition 2")

	// interval 2: delta = interval2 - interval1
	p2 := partitionMap(packets[1].Partitions)
	assertPartitionExact(t, p2[0], 2500, 250, 25, "interval 2, partition 0")
	assertPartitionExact(t, p2[1], 5000, 500, 50, "interval 2, partition 1")
	assertPartitionExact(t, p2[2], 1500, 150, 15, "interval 2, partition 2")

	// interval 3: delta = interval3 - interval2
	p3 := partitionMap(packets[2].Partitions)
	assertPartitionExact(t, p3[0], 4500, 450, 45, "interval 3, partition 0")
	assertPartitionExact(t, p3[1], 2000, 200, 20, "interval 3, partition 1")
	assertPartitionExact(t, p3[2], 5500, 550, 55, "interval 3, partition 2")
}

func TestBandwidthCollector_DeltaSumsToFinalCumulative(t *testing.T) {
	// The sum of all emitted deltas must equal the final cumulative value.
	// This is the fundamental correctness invariant.
	var packets []nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { packets = append(packets, m) },
		interval:  time.Second,
		topicName: "orders",
	}

	// 5 intervals with known cumulative values for partition 0
	cumulativeRx := []int64{1000, 3000, 7500, 12000, 20000}
	cumulativeTx := []int64{100, 300, 750, 1200, 2000}
	cumulativeMsgs := []int64{10, 30, 75, 120, 200}

	for i := range cumulativeRx {
		bc.handleStats(makeStats(
			cumulativeRx[i], cumulativeTx[i], cumulativeMsgs[i],
			0, 0, 0, // partition 1 stays at zero
			0, 0, 0, // partition 2 stays at zero
		))
	}

	if len(packets) != 5 {
		t.Fatalf("expected 5 packets, got %d", len(packets))
	}

	// sum all deltas for partition 0
	var totalRx, totalTx, totalMsgs int64
	for _, pkt := range packets {
		for _, p := range pkt.Partitions {
			if p.ID == 0 {
				totalRx += p.ReceivedBytes
				totalTx += p.TransmittedBytes
				totalMsgs += p.ReceivedMessageCount
			}
		}
	}

	// sum of deltas must equal final cumulative
	if totalRx != 20000 {
		t.Errorf("sum of ReceivedBytes deltas: got %d, want 20000", totalRx)
	}
	if totalTx != 2000 {
		t.Errorf("sum of TransmittedBytes deltas: got %d, want 2000", totalTx)
	}
	if totalMsgs != 200 {
		t.Errorf("sum of ReceivedMessageCount deltas: got %d, want 200", totalMsgs)
	}
}

func TestBandwidthCollector_CounterResetClampsToZero(t *testing.T) {
	// If librdkafka counters reset (e.g. broker restart), the delta
	// would be negative. The collector clamps to zero to avoid
	// corrupting prometheus counters.
	var packets []nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { packets = append(packets, m) },
		interval:  time.Second,
		topicName: "orders",
	}

	// first interval: cumulative 10000
	bc.handleStats(makeStats(10000, 500, 100, 0, 0, 0, 0, 0, 0))
	// second interval: counter reset to 3000 (lower than previous 10000)
	bc.handleStats(makeStats(3000, 200, 30, 0, 0, 0, 0, 0, 0))

	if len(packets) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(packets))
	}

	p0 := partitionMap(packets[1].Partitions)[0]
	if p0.ReceivedBytes != 0 {
		t.Errorf("ReceivedBytes after counter reset: got %d, want 0 (clamped)", p0.ReceivedBytes)
	}
	if p0.TransmittedBytes != 0 {
		t.Errorf("TransmittedBytes after counter reset: got %d, want 0 (clamped)", p0.TransmittedBytes)
	}
	if p0.ReceivedMessageCount != 0 {
		t.Errorf("ReceivedMessageCount after counter reset: got %d, want 0 (clamped)", p0.ReceivedMessageCount)
	}
}

func TestBandwidthCollector_SteadyStateDelta(t *testing.T) {
	// Simulate steady-state: each interval adds exactly the same
	// amount. All deltas should be identical.
	var packets []nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { packets = append(packets, m) },
		interval:  time.Second,
		topicName: "orders",
	}

	const intervals = 10
	const perIntervalRx int64 = 1024
	const perIntervalTx int64 = 128
	const perIntervalMsgs int64 = 50

	for i := 1; i <= intervals; i++ {
		bc.handleStats(makeStats(
			perIntervalRx*int64(i), perIntervalTx*int64(i), perIntervalMsgs*int64(i),
			0, 0, 0,
			0, 0, 0,
		))
	}

	if len(packets) != intervals {
		t.Fatalf("expected %d packets, got %d", intervals, len(packets))
	}

	// skip first (delta from zero = first cumulative); intervals 2-10
	// should all have the same delta
	for i := 1; i < intervals; i++ {
		p0 := partitionMap(packets[i].Partitions)[0]
		assertPartitionExact(t, p0, perIntervalRx, perIntervalTx, perIntervalMsgs,
			fmt.Sprintf("steady-state interval %d", i+1))
	}
}

func TestBandwidthCollector_NewPartitionAppearsAfterStart(t *testing.T) {
	// If a partition appears for the first time after the collector
	// has been running, it should be treated as delta from zero.
	var packets []nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { packets = append(packets, m) },
		interval:  time.Second,
		topicName: "orders",
	}

	// first interval: only partition 0
	bc.handleStats(makeStatsPartitions(map[int32][3]int64{
		0: {5000, 500, 50},
	}))

	// second interval: partition 0 grows, partition 3 appears
	bc.handleStats(makeStatsPartitions(map[int32][3]int64{
		0: {8000, 800, 80},
		3: {2000, 200, 20},
	}))

	if len(packets) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(packets))
	}

	p2 := partitionMap(packets[1].Partitions)

	// partition 0: delta = 8000-5000=3000
	assertPartitionExact(t, p2[0], 3000, 300, 30, "partition 0 delta")
	// partition 3: first appearance, delta from zero = 2000
	assertPartitionExact(t, p2[3], 2000, 200, 20, "partition 3 first appearance")
}

func TestBandwidthCollector_ZeroDeltaWhenNoTraffic(t *testing.T) {
	var packets []nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { packets = append(packets, m) },
		interval:  time.Second,
		topicName: "orders",
	}

	// two identical intervals: no new traffic
	bc.handleStats(makeStats(5000, 500, 50, 0, 0, 0, 0, 0, 0))
	bc.handleStats(makeStats(5000, 500, 50, 0, 0, 0, 0, 0, 0))

	if len(packets) != 2 {
		t.Fatalf("expected 2 packets, got %d", len(packets))
	}

	p0 := partitionMap(packets[1].Partitions)[0]
	assertPartitionExact(t, p0, 0, 0, 0, "zero delta when no traffic")
}

func TestBandwidthCollector_AggregatePartitionExcluded(t *testing.T) {
	var received nexus.BandwidthMetrics

	bc := &bandwidthCollector{
		callback:  func(m nexus.BandwidthMetrics) { received = m },
		interval:  time.Minute,
		topicName: "orders",
	}

	bc.handleStats(sampleLibrdkafkaStats)

	// should exclude aggregate partition (-1)
	if len(received.Partitions) != 3 {
		t.Fatalf("expected 3 partitions (excluding -1), got %d", len(received.Partitions))
	}

	for _, p := range received.Partitions {
		if p.ID < 0 {
			t.Errorf("aggregate partition (ID %d) should be excluded", p.ID)
		}
	}
}

// --- Adapter-level tests ---

func TestAdapter_WithBandwidthInterval_Kafka(t *testing.T) {
	t.Run("valid interval", func(t *testing.T) {
		a := NewCustom()
		a.WithBandwidthInterval(30 * time.Second)

		if a.bwCollector == nil {
			t.Fatal("expected bwCollector to be created")
		}
		if a.bwCollector.interval != 30*time.Second {
			t.Errorf("expected interval 30s, got %v", a.bwCollector.interval)
		}
	})

	t.Run("zero uses default", func(t *testing.T) {
		a := NewCustom()
		a.WithBandwidthInterval(0)

		if a.bwCollector.interval != nexus.DefaultBandwidthInterval {
			t.Errorf("expected default interval, got %v", a.bwCollector.interval)
		}
	})

	t.Run("invalid interval panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for invalid interval")
			}
		}()
		a := NewCustom()
		a.WithBandwidthInterval(-time.Second)
	})
}

func TestAdapter_StatsInterval_Kafka(t *testing.T) {
	t.Run("with collector", func(t *testing.T) {
		a := NewCustom()
		a.WithBandwidthInterval(5 * time.Minute)

		if a.StatsInterval() != 5*time.Minute {
			t.Errorf("expected 5m, got %v", a.StatsInterval())
		}
	})

	t.Run("without collector returns default", func(t *testing.T) {
		a := NewCustom()
		if a.StatsInterval() != nexus.DefaultBandwidthInterval {
			t.Errorf("expected default, got %v", a.StatsInterval())
		}
	})
}

func TestAdapter_SetBandwidthCallback_Kafka_NilCollector(t *testing.T) {
	// should not panic when no bandwidth collector configured
	a := NewCustom()
	a.SetBandwidthCallback(func(nexus.BandwidthMetrics) {})
}

func TestSetBandwidthCallback_WithCollector_AssignsCallback(t *testing.T) {
	adapter := NewCustom()
	adapter.bwCollector = &bandwidthCollector{interval: time.Minute}

	called := false
	cb := func(_ nexus.BandwidthMetrics) { called = true }
	adapter.SetBandwidthCallback(cb)

	if adapter.bwCollector.callback == nil {
		t.Fatal("expected callback to be assigned")
	}
	// invoke through the stored callback to confirm wiring
	adapter.bwCollector.callback(nexus.BandwidthMetrics{})
	if !called {
		t.Error("expected stored callback to be the one we set")
	}
}

func TestCreateConsumer_WithBandwidthCollector_SetsTopic(t *testing.T) {
	adapter := NewCustom()
	adapter.bwCollector = &bandwidthCollector{interval: time.Minute}

	mockCtx := context.Background()
	mockLog := &mockLogger{}
	mockConsumer := &mockAdaptedConsumer{ctx: mockCtx, logger: mockLog}
	builder := &mockConsumerBuilder{
		topicName:       "bw-topic",
		adaptedConsumer: mockConsumer,
	}

	adapter.CreateConsumer(builder)

	if adapter.bwCollector.topicName != "bw-topic" {
		t.Errorf("expected bwCollector.topicName 'bw-topic', got %q", adapter.bwCollector.topicName)
	}
}

func TestPoll_StatsEvent_WithCollector(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.bwCollector = &bandwidthCollector{interval: time.Minute}

	event := &kafka.Stats{}
	adapter.pollFn = func(_ int) kafka.Event { return event }

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if msg != nil {
		t.Error("expected nil message")
	}
	if ok {
		t.Error("expected ok=false")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPoll_StatsEvent_NoCollector(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	// bwCollector intentionally nil

	event := &kafka.Stats{}
	adapter.pollFn = func(_ int) kafka.Event { return event }

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if msg != nil {
		t.Error("expected nil message")
	}
	if ok {
		t.Error("expected ok=false")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestGenerateUUID_Kafka(t *testing.T) {
	uuid := generateUUID()
	if len(uuid) != 36 {
		t.Errorf("expected UUID length 36, got %d: %q", len(uuid), uuid)
	}
	if uuid[14] != '4' {
		t.Errorf("expected version '4' at position 14, got %c", uuid[14])
	}
}

// --- helpers ---

func partitionMap(parts []nexus.PartitionBandwidth) map[int32]nexus.PartitionBandwidth {
	m := make(map[int32]nexus.PartitionBandwidth, len(parts))
	for _, p := range parts {
		m[p.ID] = p
	}
	return m
}

func assertPartitionExact(t *testing.T, p nexus.PartitionBandwidth, wantRx, wantTx, wantMsgs int64, context string) {
	t.Helper()
	if p.ReceivedBytes != wantRx {
		t.Errorf("%s: ReceivedBytes got %d, want %d", context, p.ReceivedBytes, wantRx)
	}
	if p.TransmittedBytes != wantTx {
		t.Errorf("%s: TransmittedBytes got %d, want %d", context, p.TransmittedBytes, wantTx)
	}
	if p.ReceivedMessageCount != wantMsgs {
		t.Errorf("%s: ReceivedMessageCount got %d, want %d", context, p.ReceivedMessageCount, wantMsgs)
	}
}

// makeStats builds a librdkafka stats JSON blob with 3 partitions and
// the given cumulative values. Partition -1 aggregate is included for realism.
func makeStats(
	p0rx, p0tx, p0msgs,
	p1rx, p1tx, p1msgs,
	p2rx, p2tx, p2msgs int64,
) string {
	return fmt.Sprintf(`{
  "brokers": {
    "broker-1:9092/1": {"nodeid": 1, "name": "b1", "host": "broker-1", "port": 9092, "rack": "", "txbytes": 0, "rxbytes": 0}
  },
  "topics": {
    "orders": {
      "partitions": {
        "0": {"partition": 0, "leader": 1, "rxbytes": %d, "txbytes": %d, "rxmsgs": %d},
        "1": {"partition": 1, "leader": 1, "rxbytes": %d, "txbytes": %d, "rxmsgs": %d},
        "2": {"partition": 2, "leader": 1, "rxbytes": %d, "txbytes": %d, "rxmsgs": %d},
        "-1": {"partition": -1, "leader": -1, "rxbytes": %d, "txbytes": %d, "rxmsgs": %d}
      }
    }
  }
}`, p0rx, p0tx, p0msgs,
		p1rx, p1tx, p1msgs,
		p2rx, p2tx, p2msgs,
		p0rx+p1rx+p2rx, p0tx+p1tx+p2tx, p0msgs+p1msgs+p2msgs)
}

// makeStatsPartitions builds a stats JSON with an arbitrary set of partitions.
func makeStatsPartitions(partitions map[int32][3]int64) string {
	parts := ""
	var totalRx, totalTx, totalMsgs int64
	first := true
	for id, vals := range partitions {
		if !first {
			parts += ",\n"
		}
		parts += fmt.Sprintf(`        "%d": {"partition": %d, "leader": 1, "rxbytes": %d, "txbytes": %d, "rxmsgs": %d}`,
			id, id, vals[0], vals[1], vals[2])
		totalRx += vals[0]
		totalTx += vals[1]
		totalMsgs += vals[2]
		first = false
	}
	parts += fmt.Sprintf(`,
        "-1": {"partition": -1, "leader": -1, "rxbytes": %d, "txbytes": %d, "rxmsgs": %d}`,
		totalRx, totalTx, totalMsgs)

	return fmt.Sprintf(`{
  "brokers": {
    "broker-1:9092/1": {"nodeid": 1, "name": "b1", "host": "broker-1", "port": 9092, "rack": "", "txbytes": 0, "rxbytes": 0}
  },
  "topics": {
    "orders": {
      "partitions": {
%s
      }
    }
  }
}`, parts)
}
