// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

// --- Mock BrokerPort for SimpleConsumer testing ---

type mockBrokerPort struct {
	subscribeErr   error
	unsubscribeErr error
	pollMessages   []*kafka.Message
	pollIndex      int
	pollErr        error
	commitErr      error
	ackErr         error

	subscribeCalled   bool
	unsubscribeCalled bool
	commitCalled      bool
	committedMessages []*nexus.Message[*kafka.Message]
}

func (m *mockBrokerPort) Subscribe() error {
	m.subscribeCalled = true
	return m.subscribeErr
}

func (m *mockBrokerPort) Unsubscribe() error {
	m.unsubscribeCalled = true
	return m.unsubscribeErr
}

func (m *mockBrokerPort) Poll(_ time.Duration) (*kafka.Message, bool, error) {
	if m.pollErr != nil {
		return nil, false, m.pollErr
	}
	if m.pollIndex < len(m.pollMessages) {
		msg := m.pollMessages[m.pollIndex]
		m.pollIndex++
		return msg, true, nil
	}
	return nil, false, nil
}

func (m *mockBrokerPort) ExtractEnvelope(msg *kafka.Message) nexus.Envelope {
	return nexus.Envelope{
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       string(msg.Key),
	}
}

func (m *mockBrokerPort) CommitOffsets(msgs []*nexus.Message[*kafka.Message]) ([]*nexus.Message[*kafka.Message], error) {
	m.commitCalled = true
	m.committedMessages = append(m.committedMessages, msgs...)
	if m.commitErr != nil {
		return msgs, m.commitErr
	}
	return nil, nil
}

func (m *mockBrokerPort) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return m.ackErr
}

func (m *mockBrokerPort) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

func (m *mockBrokerPort) ConsumerGroup() string {
	return ""
}

// --- SimpleConsumerBuilder Tests ---

func TestNewSimpleConsumerBuilder(t *testing.T) {
	process := func(_ context.Context, _ *nexus.Message[*kafka.Message]) error {
		return nil
	}

	builder := NewSimpleConsumerBuilder(testTopicName, process)

	if builder == nil {
		t.Fatal("expected builder, got nil")
	}
	if builder.topicName != testTopicName {
		t.Errorf("expected topic '%s', got '%s'", testTopicName, builder.topicName)
	}
	if builder.process == nil {
		t.Error("expected process function to be set")
	}
	if builder.logger == nil {
		t.Error("expected default logger to be set")
	}
	if builder.ctx == nil {
		t.Error("expected default context to be set")
	}
}

func TestSimpleConsumerBuilder_TopicName(t *testing.T) {
	builder := NewSimpleConsumerBuilder("my-topic", nil)

	if builder.TopicName() != "my-topic" {
		t.Errorf("expected 'my-topic', got '%s'", builder.TopicName())
	}
}

func TestSimpleConsumerBuilder_WithLogger(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	customLogger := &mockLogger{}

	result := builder.WithLogger(customLogger)

	if result != builder {
		t.Error("expected fluent interface to return same builder")
	}
	if builder.logger != customLogger {
		t.Error("expected custom logger to be set")
	}
}

func TestSimpleConsumerBuilder_WithContext(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	customCtx := context.WithValue(context.Background(), "key", "value") //nolint:revive,staticcheck // test context

	result := builder.WithContext(customCtx)

	if result != builder {
		t.Error("expected fluent interface to return same builder")
	}
	if builder.ctx != customCtx {
		t.Error("expected custom context to be set")
	}
}

func TestSimpleConsumerBuilder_Build(t *testing.T) {
	processCalled := false
	process := func(_ context.Context, _ *nexus.Message[*kafka.Message]) error {
		processCalled = true
		return nil
	}

	builder := NewSimpleConsumerBuilder(testTopicName, process)
	mockBroker := &mockBrokerPort{}

	consumer := builder.Build(mockBroker)

	if consumer == nil {
		t.Fatal("expected consumer, got nil")
	}
	if consumer.Context() == nil {
		t.Error("expected context to be returned")
	}
	if consumer.Logger() == nil {
		t.Error("expected logger to be returned")
	}

	// verify consumer is SimpleConsumer
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}
	if sc.topicName != testTopicName {
		t.Errorf("expected topic '%s', got '%s'", testTopicName, sc.topicName)
	}

	// verify process function is wired
	_ = sc.process(context.Background(), nil)
	if !processCalled {
		t.Error("expected process function to be callable")
	}
}

// --- SimpleConsumer Tests ---

func TestSimpleConsumer_TopicName(t *testing.T) {
	builder := NewSimpleConsumerBuilder("my-topic", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	if sc.TopicName() != "my-topic" {
		t.Errorf("expected 'my-topic', got '%s'", sc.TopicName())
	}
}

func TestSimpleConsumer_Subscribe_Success(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	err := sc.Subscribe()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockBroker.subscribeCalled {
		t.Error("expected broker.Subscribe to be called")
	}
	if !sc.running.Load() {
		t.Error("expected running to be true")
	}

	// cleanup
	_ = sc.Shutdown()
}

func TestSimpleConsumer_Subscribe_Error(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{
		subscribeErr: errors.New("subscribe failed"),
	}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	err := sc.Subscribe()

	if err == nil {
		t.Error("expected error")
	}
	if !strings.Contains(err.Error(), "subscribe failed") {
		t.Errorf("expected error containing 'subscribe failed', got '%v'", err)
	}
}

func TestSimpleConsumer_Shutdown(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	// subscribe first
	_ = sc.Subscribe()

	// shutdown
	err := sc.Shutdown()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sc.running.Load() {
		t.Error("expected running to be false after shutdown")
	}
	if !mockBroker.unsubscribeCalled {
		t.Error("expected broker.Unsubscribe to be called")
	}
}

func TestSimpleConsumer_Shutdown_WithoutSubscribe(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	// shutdown without subscribing (done channel is nil)
	err := sc.Shutdown()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockBroker.unsubscribeCalled {
		t.Error("expected broker.Unsubscribe to be called")
	}
}

func TestSimpleConsumer_TriggerRebalance(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	info := []nexus.RebalanceInfo{
		{Partition: 0, CommittedOffset: 100},
		{Partition: 1, CommittedOffset: 200},
	}

	err := sc.TriggerRebalance(nexus.Assign, info)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSimpleConsumer_TriggerRebalance_Error(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{
		ackErr: errors.New("ack failed"),
	}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	err := sc.TriggerRebalance(nexus.Revoke, nil)

	if err == nil {
		t.Error("expected error")
	}
}

func TestSimpleConsumer_Metrics_Empty(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	metrics := sc.Metrics()

	if metrics == nil {
		t.Error("expected non-nil slice")
	}
	if len(metrics) != 0 {
		t.Errorf("expected 0 metrics, got %d", len(metrics))
	}
}

func TestSimpleConsumer_MetricsCount_Empty(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	count := sc.MetricsCount()

	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}

func TestSimpleConsumer_PollLoop_ProcessesMessages(t *testing.T) {
	var processedCount atomic.Int32
	process := func(_ context.Context, _ *nexus.Message[*kafka.Message]) error {
		processedCount.Add(1)
		return nil
	}

	topic := "test"
	mockBroker := &mockBrokerPort{
		pollMessages: []*kafka.Message{
			{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
				Key:            []byte("key-1"),
				Value:          []byte("value-1"),
			},
			{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 1},
				Key:            []byte("key-2"),
				Value:          []byte("value-2"),
			},
		},
	}

	builder := NewSimpleConsumerBuilder("test", process)
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	_ = sc.Subscribe()

	// wait for messages to be processed
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processedCount.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = sc.Shutdown()

	if processedCount.Load() != 2 {
		t.Errorf("expected 2 messages processed, got %d", processedCount.Load())
	}

	// verify metrics were collected
	metrics := sc.Metrics()
	if len(metrics) != 2 {
		t.Errorf("expected 2 metrics, got %d", len(metrics))
	}

	// verify commits were made
	if !mockBroker.commitCalled {
		t.Error("expected commits to be made")
	}
}

func TestSimpleConsumer_PollLoop_HandlesProcessError(t *testing.T) {
	var processedCount atomic.Int32
	process := func(_ context.Context, _ *nexus.Message[*kafka.Message]) error {
		processedCount.Add(1)
		return errors.New("process error") // error is ignored by SimpleConsumer
	}

	topic := "test"
	mockBroker := &mockBrokerPort{
		pollMessages: []*kafka.Message{
			{
				TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: 0, Offset: 0},
				Key:            []byte("key"),
			},
		},
	}

	builder := NewSimpleConsumerBuilder("test", process)
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	_ = sc.Subscribe()

	// wait for message to be processed
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if processedCount.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = sc.Shutdown()

	// message should still be processed (error ignored)
	if processedCount.Load() != 1 {
		t.Errorf("expected 1 message processed, got %d", processedCount.Load())
	}

	// metrics should still be collected
	if sc.MetricsCount() != 1 {
		t.Errorf("expected 1 metric, got %d", sc.MetricsCount())
	}
}

func TestSimpleConsumer_PollLoop_HandlesPollError(t *testing.T) {
	process := func(_ context.Context, _ *nexus.Message[*kafka.Message]) error {
		return nil
	}

	// use a broker that returns errors
	mockBroker := &mockBrokerPortWithPollError{
		pollErrCount: 2, // return errors for first 2 polls
	}

	builder := NewSimpleConsumerBuilder("test", process)
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	_ = sc.Subscribe()
	time.Sleep(300 * time.Millisecond)
	_ = sc.Shutdown()

	// poll loop should have continued despite errors (no panic, graceful shutdown)
	if mockBroker.pollCount < 2 {
		t.Errorf("expected at least 2 poll calls, got %d", mockBroker.pollCount)
	}
}

// mockBrokerPortWithPollError returns errors for the first N polls
type mockBrokerPortWithPollError struct {
	pollErrCount int
	pollCount    int
}

func (m *mockBrokerPortWithPollError) Subscribe() error   { return nil }
func (m *mockBrokerPortWithPollError) Unsubscribe() error { return nil }
func (m *mockBrokerPortWithPollError) ExtractEnvelope(_ *kafka.Message) nexus.Envelope {
	return nexus.Envelope{}
}
func (m *mockBrokerPortWithPollError) CommitOffsets(_ []*nexus.Message[*kafka.Message]) ([]*nexus.Message[*kafka.Message], error) {
	return nil, nil
}
func (m *mockBrokerPortWithPollError) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}
func (m *mockBrokerPortWithPollError) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}
func (m *mockBrokerPortWithPollError) ConsumerGroup() string {
	return ""
}
func (m *mockBrokerPortWithPollError) Poll(_ time.Duration) (*kafka.Message, bool, error) {
	m.pollCount++
	if m.pollCount <= m.pollErrCount {
		return nil, false, errors.New("poll error")
	}
	return nil, false, nil
}

func TestSimpleConsumer_Metrics_ReturnsCopy(t *testing.T) {
	builder := NewSimpleConsumerBuilder("test", nil)
	mockBroker := &mockBrokerPort{}
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	// add some metrics manually
	sc.mu.Lock()
	sc.metrics = append(sc.metrics, nexus.Metrics{Partition: 0, Offset: 100})
	sc.mu.Unlock()

	// get metrics
	metrics1 := sc.Metrics()
	metrics2 := sc.Metrics()

	// modify first copy
	metrics1[0].Offset = 999

	// second copy should be unaffected
	if metrics2[0].Offset != 100 {
		t.Error("expected Metrics() to return a copy, not a reference")
	}
}

func TestSimpleConsumer_PollLoop_ContextCancellation(t *testing.T) {
	process := func(_ context.Context, _ *nexus.Message[*kafka.Message]) error {
		return nil
	}

	// broker that always returns nil (no messages)
	mockBroker := &mockBrokerPort{}

	// create with cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	builder := NewSimpleConsumerBuilder("test", process).WithContext(ctx)
	consumer := builder.Build(mockBroker)
	sc, ok := consumer.(*SimpleConsumer)
	if !ok {
		t.Fatal("expected *SimpleConsumer")
	}

	_ = sc.Subscribe()

	// give poll loop time to start
	time.Sleep(50 * time.Millisecond)

	// cancel context - should stop poll loop
	cancel()

	// wait for shutdown
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if !sc.running.Load() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// verify poll loop stopped via context cancellation
	// (done channel should be closed)
	select {
	case <-sc.done:
		// good - poll loop exited
	case <-time.After(500 * time.Millisecond):
		t.Error("expected poll loop to exit after context cancellation")
	}
}
