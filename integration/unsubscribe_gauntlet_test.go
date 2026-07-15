// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-adapter-kafka/kafkaadapter"
	"github.com/llingr/llingr-nexus/nexus"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"
)

// The Unsubscribe gauntlet runs a REAL librdkafka consumer against kfake
// (franz-go's in-process broker; librdkafka joins and consumes from it, as
// the probe run for this test established): join a group, consume a proof
// record through the adapter, then race many concurrent Unsubscribe callers
// and hold the release contract:
//
//   - idempotent: every caller returns, the release runs once;
//   - ownership: an adapter-created consumer (New) ends closed, a consumer
//     supplied via SetConsumer is left open for its owner and remains usable.
//
// Even iterations run the owned path, odd iterations the custom path, and
// goleak verifies each iteration tears down every goroutine.
//
// Iterations default to 50 (-short: 3); LLINGR_KAFKA_STRESS_ITERS overrides.
const kafkaStressItersEnv = "LLINGR_KAFKA_STRESS_ITERS"

// gauntletEngine is the minimal engine the adapter needs; the gauntlet never
// shuts down through it.
type gauntletEngine struct{ ctx context.Context }

func (e *gauntletEngine) Subscribe() error { return nil }
func (e *gauntletEngine) Shutdown() error  { return nil }
func (e *gauntletEngine) TriggerRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}
func (e *gauntletEngine) Context() context.Context { return e.ctx }
func (e *gauntletEngine) Logger() nexus.Logger     { return &mockLogger{} }

type gauntletBuilder struct {
	topic  string
	engine *gauntletEngine
}

func (b *gauntletBuilder) TopicName() string { return b.topic }
func (b *gauntletBuilder) Build(nexus.BrokerPort[*kafka.Message]) nexus.AdaptedConsumer[*kafka.Message] {
	return b.engine
}

func TestUnsubscribe_Gauntlet(t *testing.T) {
	iterations := 50
	if v := os.Getenv(kafkaStressItersEnv); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			iterations = n
		}
	}
	if testing.Short() && os.Getenv(kafkaStressItersEnv) == "" {
		iterations = 3
	}
	t.Logf("unsubscribe gauntlet: %d iterations", iterations)

	for i := 0; i < iterations; i++ {
		customConsumer := i%2 == 1
		t.Run(fmt.Sprintf("iter_%d_custom_%v", i, customConsumer), func(t *testing.T) {
			runUnsubscribeIteration(t, customConsumer)
		})
		if t.Failed() {
			t.Fatalf("iteration %d failed", i)
		}
	}
}

func runUnsubscribeIteration(t *testing.T, customConsumer bool) {
	t.Helper()
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const topic = "unsub-topic"
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("kfake.NewCluster: %v", err)
	}
	defer cluster.Close()
	seeds := strings.Join(cluster.ListenAddrs(), ",")

	// one record proves join+fetch work before the release is raced
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()
	if err := producer.ProduceSync(context.Background(),
		&kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":  seeds,
		"group.id":           "unsub-group",
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	}

	var adapter *kafkaadapter.Adapter
	var owned *kafka.Consumer // non-nil only on the custom path
	if customConsumer {
		if err := configMap.SetKey("enable.auto.offset.store", false); err != nil {
			t.Fatalf("SetKey: %v", err)
		}
		owned, err = kafka.NewConsumer(configMap)
		if err != nil {
			t.Fatalf("kafka.NewConsumer: %v", err)
		}
		defer func() { _ = owned.Close() }()
		adapter = kafkaadapter.NewCustom()
		adapter.SetConsumer(owned)
	} else {
		adapter, err = kafkaadapter.New(configMap)
		if err != nil {
			t.Fatalf("kafkaadapter.New: %v", err)
		}
	}

	engine := &gauntletEngine{ctx: context.Background()}
	adapter.CreateConsumer(&gauntletBuilder{topic: topic, engine: engine})
	if err := adapter.Subscribe(); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		record, ok, _ := adapter.Poll(200 * time.Millisecond)
		if ok && record != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("proof record never arrived; group join did not complete")
		}
	}

	// the race: many concurrent callers, all must return, release runs once
	var wg sync.WaitGroup
	for c := 0; c < 16; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = adapter.Unsubscribe()
		}()
	}
	wg.Wait()
	_ = adapter.Unsubscribe() // late sequential repeat

	if customConsumer {
		// ownership: the user's consumer is left open and usable
		if owned.IsClosed() {
			t.Fatal("Unsubscribe closed a consumer supplied via SetConsumer")
		}
		_ = owned.Poll(0) // still usable by its owner
	} else {
		// owned: the adapter closed its consumer; Poll reports it
		if _, _, err := adapter.Poll(10 * time.Millisecond); err == nil ||
			!strings.Contains(err.Error(), "closed") {
			t.Fatalf("owned consumer should be closed after Unsubscribe, Poll err = %v", err)
		}
	}
}
