// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

// --- Subscribe Tests ---

func TestSubscribe_WithCallbackPolicy(t *testing.T) {
	adapter := NewCustom(FromRebalanceCallback)
	adapter.topicName = testTopicName
	adapter.logger = &mockLogger{}
	adapter.ctx = context.Background()

	var capturedCb kafka.RebalanceCb
	adapter.subscribeFn = func(topic string, cb kafka.RebalanceCb) error {
		if topic != testTopicName {
			t.Errorf("expected topic 'test-topic', got '%s'", topic)
		}
		capturedCb = cb
		return nil
	}

	err := adapter.Subscribe()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedCb == nil {
		t.Error("expected rebalance callback to be set for FromRebalanceCallback policy")
	}
}

func TestSubscribe_CallbackInvocation(t *testing.T) {
	adapter := NewCustom(FromRebalanceCallback)
	adapter.topicName = testTopicName
	adapter.logger = &mockLogger{}
	adapter.ctx = context.Background()

	// mock rebalance handling dependencies
	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return "COOPERATIVE" }
	adapter.incrementalAssignFn = func(_ []kafka.TopicPartition) error { return nil }

	var capturedCb kafka.RebalanceCb
	adapter.subscribeFn = func(_ string, cb kafka.RebalanceCb) error {
		capturedCb = cb
		return nil
	}

	err := adapter.Subscribe()
	if err != nil {
		t.Fatalf("subscribe error: %v", err)
	}

	// invoke the captured callback with an assigned partitions event
	topic := testTopicName
	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Topic: &topic, Partition: 0, Offset: 0},
		},
	}

	err = capturedCb(nil, event)
	if err != nil {
		t.Fatalf("callback error: %v", err)
	}

	if !mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance to be called via callback")
	}
}

func TestSubscribe_WithPollPolicy(t *testing.T) {
	adapter := NewCustom(FromPoll)
	adapter.topicName = testTopicName
	adapter.logger = &mockLogger{}
	adapter.ctx = context.Background()

	var capturedCb kafka.RebalanceCb
	adapter.subscribeFn = func(_ string, cb kafka.RebalanceCb) error {
		capturedCb = cb
		return nil
	}

	err := adapter.Subscribe()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedCb != nil {
		t.Error("expected no rebalance callback for FromPoll policy")
	}
}

func TestSubscribe_Error(t *testing.T) {
	adapter := NewCustom()
	adapter.topicName = testTopicName
	adapter.logger = &mockLogger{}
	adapter.ctx = context.Background()

	expectedErr := errors.New("subscribe failed")
	adapter.subscribeFn = func(_ string, _ kafka.RebalanceCb) error {
		return expectedErr
	}

	err := adapter.Subscribe()
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

// --- Unsubscribe Tests ---

func TestUnsubscribe_NilConsumer(t *testing.T) {
	adapter := NewCustom()

	// should not panic with nil consumer (closeFn is nil)
	err := adapter.Unsubscribe()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestUnsubscribe_WithConsumer(t *testing.T) {
	adapter := NewCustom()

	unsubscribeCalled := false
	closeCalled := false

	adapter.unsubscribeFn = func() error {
		unsubscribeCalled = true
		return nil
	}
	adapter.closeFn = func() error {
		closeCalled = true
		return nil
	}

	err := adapter.Unsubscribe()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !unsubscribeCalled {
		t.Error("expected unsubscribeFn to be called")
	}
	if !closeCalled {
		t.Error("expected closeFn to be called")
	}
}

func TestUnsubscribe_IgnoresErrors(t *testing.T) {
	adapter := NewCustom()

	adapter.unsubscribeFn = func() error {
		return errors.New("unsubscribe error")
	}
	adapter.closeFn = func() error {
		return errors.New("close error")
	}

	// errors are ignored, should still return nil
	err := adapter.Unsubscribe()
	if err != nil {
		t.Errorf("expected nil error, got: %v", err)
	}
}

// --- Poll Tests ---

func TestPoll_WhenClosed(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return true }

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if msg != nil {
		t.Error("expected nil message")
	}
	if ok {
		t.Error("expected ok=false")
	}
	if err == nil {
		t.Error("expected error when closed")
	}
}

func TestPoll_NilEvent(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.pollFn = func(_ int) kafka.Event { return nil }

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

func TestPoll_Message(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	topic := testTopicName
	expectedMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    42,
		},
		Value: []byte("test-value"),
	}

	adapter.pollFn = func(_ int) kafka.Event { return expectedMsg }

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if msg != expectedMsg {
		t.Error("expected message to be returned")
	}
	if !ok {
		t.Error("expected ok=true")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestPoll_MessageWithPartitionError(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	topic := testTopicName
	partitionErr := kafka.NewError(kafka.ErrUnknownPartition, "unknown partition", false)
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Error:     partitionErr,
		},
	}

	adapter.pollFn = func(_ int) kafka.Event { return msg }

	returnedMsg, ok, err := adapter.Poll(100 * time.Millisecond)

	if returnedMsg != msg {
		t.Error("expected message to be returned even with error")
	}
	if ok {
		t.Error("expected ok=false for partition error")
	}
	if !errors.Is(err, partitionErr) {
		t.Errorf("expected partition error, got %v", err)
	}
}

func TestPoll_KafkaError(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	kafkaErr := kafka.NewError(kafka.ErrBrokerNotAvailable, "broker not available", false)
	adapter.pollFn = func(_ int) kafka.Event { return kafkaErr }

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if msg != nil {
		t.Error("expected nil message")
	}
	if ok {
		t.Error("expected ok=false")
	}
	if !errors.Is(err, kafkaErr) {
		t.Errorf("expected kafka error, got %v", err)
	}
}

func TestPoll_OffsetsCommitted(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	event := kafka.OffsetsCommitted{}
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

func TestPoll_AssignedPartitions_FromPoll(t *testing.T) {
	adapter := NewCustom(FromPoll)
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	assignCalled := false
	adapter.assignFn = func(_ []kafka.TopicPartition) error {
		assignCalled = true
		return nil
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 0, Offset: 100},
			{Partition: 1, Offset: 200},
		},
	}
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
	if !mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance to be called")
	}
	if mockConsumer.lastRebalanceType != nexus.Assign {
		t.Errorf("expected Assign, got %v", mockConsumer.lastRebalanceType)
	}
	if !assignCalled {
		t.Error("expected assignFn to be called")
	}
}

func TestPoll_AssignedPartitions_FromCallback_Skipped(t *testing.T) {
	adapter := NewCustom(FromRebalanceCallback)
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}
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
	if mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance NOT to be called for callback policy")
	}
}

func TestPoll_RevokedPartitions_FromPoll(t *testing.T) {
	adapter := NewCustom(FromPoll)
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	unassignCalled := false
	adapter.unassignFn = func() error {
		unassignCalled = true
		return nil
	}

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}
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
	if !mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance to be called")
	}
	if mockConsumer.lastRebalanceType != nexus.Revoke {
		t.Errorf("expected Revoke, got %v", mockConsumer.lastRebalanceType)
	}
	if !unassignCalled {
		t.Error("expected unassignFn to be called")
	}
}

func TestPoll_RevokedPartitions_FromCallback_Skipped(t *testing.T) {
	adapter := NewCustom(FromRebalanceCallback)
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}
	adapter.pollFn = func(_ int) kafka.Event { return event }

	_, _, _ = adapter.Poll(100 * time.Millisecond)

	if mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance NOT to be called for callback policy")
	}
}

func TestPoll_UnknownEvent(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	// use a custom event type that we don't handle explicitly
	event := &unknownEvent{}
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

// unknownEvent implements kafka.Event for testing unknown event handling
type unknownEvent struct{}

func (e *unknownEvent) String() string { return "unknown" }

func TestPoll_TimeoutConversion(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }

	var capturedTimeout int
	adapter.pollFn = func(timeout int) kafka.Event {
		capturedTimeout = timeout
		return nil
	}

	_, _, _ = adapter.Poll(150 * time.Millisecond)

	if capturedTimeout != 150 {
		t.Errorf("expected timeout 150ms, got %dms", capturedTimeout)
	}
}

// --- ExtractEnvelope Tests ---

func TestExtractEnvelope_UTF8Key(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	msg := &kafka.Message{
		Key: []byte("user-123"),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 5,
			Offset:    1000,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	if envelope.Key != "user-123" {
		t.Errorf("expected key 'user-123', got '%s'", envelope.Key)
	}
	if envelope.Partition != 5 {
		t.Errorf("expected partition 5, got %d", envelope.Partition)
	}
	if envelope.Offset != 1000 {
		t.Errorf("expected offset 1000, got %d", envelope.Offset)
	}
	if envelope.Ctx != adapter.ctx {
		t.Error("expected context to be set")
	}
}

func TestExtractEnvelope_BinaryKey(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	// invalid UTF-8 sequence
	binaryKey := []byte{0x80, 0x81, 0x82, 0x83}
	topic := testTopicName
	msg := &kafka.Message{
		Key: binaryKey,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	expectedKey := base64.StdEncoding.EncodeToString(binaryKey)
	if envelope.Key != expectedKey {
		t.Errorf("expected base64 key '%s', got '%s'", expectedKey, envelope.Key)
	}
}

func TestExtractEnvelope_EmptyKey(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	msg := &kafka.Message{
		Key: nil,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 7,
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	if envelope.Key != "7" {
		t.Errorf("expected key '7' (partition number), got '%s'", envelope.Key)
	}
}

func TestExtractEnvelope_EmptyStringKey(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	msg := &kafka.Message{
		Key: []byte{},
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 3,
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	if envelope.Key != "3" {
		t.Errorf("expected key '3' (partition number), got '%s'", envelope.Key)
	}
}

func TestExtractEnvelope_UnicodeKey(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	msg := &kafka.Message{
		Key: []byte("用户-123"), // Chinese characters
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	if envelope.Key != "用户-123" {
		t.Errorf("expected key '用户-123', got '%s'", envelope.Key)
	}
}

// --- CommitOffsets Tests ---

func TestCommitOffsets_EmptyMessages(t *testing.T) {
	adapter := NewCustom()

	failed, err := adapter.CommitOffsets(nil)

	if failed != nil {
		t.Error("expected nil failed messages")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestCommitOffsets_Success(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	var committedOffsets []kafka.TopicPartition
	adapter.commitOffsetsFn = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		committedOffsets = offsets
		return offsets, nil
	}

	topic := testTopicName
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 2,
			Offset:    99,
		},
	}

	msg := &nexus.Message[*kafka.Message]{
		Partition: 2,
		Offset:    99,
		Payload:   &kafkaMsg,
	}

	failed, err := adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Error("expected nil failed messages on success")
	}
	if len(committedOffsets) != 1 {
		t.Fatalf("expected 1 committed offset, got %d", len(committedOffsets))
	}
	// Kafka commits "next offset to read", so offset should be incremented
	if committedOffsets[0].Offset != 100 {
		t.Errorf("expected committed offset 100, got %d", committedOffsets[0].Offset)
	}
}

func TestCommitOffsets_MultipleMessages(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	var committedOffsets []kafka.TopicPartition
	adapter.commitOffsetsFn = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		committedOffsets = offsets
		return offsets, nil
	}

	topic := testTopicName
	messages := make([]*nexus.Message[*kafka.Message], 3)
	for i := range 3 {
		partition := int32(i) // #nosec G115 -- test code with small loop values
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    kafka.Offset(i * 100),
			},
		}
		messages[i] = &nexus.Message[*kafka.Message]{
			Partition: partition,
			Offset:    int64(i * 100),
			Payload:   &kafkaMsg,
		}
	}

	failed, err := adapter.CommitOffsets(messages)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if failed != nil {
		t.Error("expected nil failed messages on success")
	}
	if len(committedOffsets) != 3 {
		t.Fatalf("expected 3 committed offsets, got %d", len(committedOffsets))
	}

	// verify each offset is incremented by 1
	expectedOffsets := []kafka.Offset{1, 101, 201}
	for i, tp := range committedOffsets {
		if tp.Offset != expectedOffsets[i] {
			t.Errorf("offset[%d]: expected %d, got %d", i, expectedOffsets[i], tp.Offset)
		}
	}
}

func TestCommitOffsets_Error(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	expectedErr := errors.New("commit failed")
	adapter.commitOffsetsFn = func(_ []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		return nil, expectedErr
	}

	topic := testTopicName
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
	}
	msg := &nexus.Message[*kafka.Message]{
		Payload: &kafkaMsg,
	}

	failed, err := adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if len(failed) != 1 {
		t.Errorf("expected 1 failed message, got %d", len(failed))
	}
}

// --- ConsumerGroup Tests ---

func TestConsumerGroup_FromConfigMap(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "my-group-id",
	}
	adapter, err := New(configMap)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() { _ = adapter.kafkaConsumer.Close() }()

	if got := adapter.ConsumerGroup(); got != "my-group-id" {
		t.Errorf("expected 'my-group-id', got %q", got)
	}
}

func TestConsumerGroup_EmptyForNewCustom(t *testing.T) {
	adapter := NewCustom()
	if got := adapter.ConsumerGroup(); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

// --- AckRebalance Tests ---

func TestAckRebalance_NoOp(t *testing.T) {
	adapter := NewCustom()

	err := adapter.AckRebalance(nexus.Assign, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	err = adapter.AckRebalance(nexus.Revoke, []nexus.RebalanceInfo{{Partition: 0}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// --- BrokerQuery Tests ---

func TestBrokerQuery_NoOp(t *testing.T) {
	adapter := NewCustom()

	resp, err := adapter.BrokerQuery(nexus.QueryRequest{})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// QueryResponse contains map, can't compare directly - check fields
	if resp.QueryType != 0 {
		t.Errorf("expected QueryType 0, got %v", resp.QueryType)
	}
	if resp.TopicName != "" {
		t.Errorf("expected empty TopicName, got %s", resp.TopicName)
	}
	if resp.OffsetsByPartition != nil {
		t.Errorf("expected nil OffsetsByPartition, got %v", resp.OffsetsByPartition)
	}
	if resp.Data != nil {
		t.Errorf("expected nil Data, got %v", resp.Data)
	}
}

// --- Interface Compliance ---

func TestAdapter_ImplementsBrokerPort(_ *testing.T) {
	var _ nexus.BrokerPort[*kafka.Message] = (*Adapter)(nil)
}

// --- CommitOffsets Edge Cases ---
// These tests verify edge cases that mutation testing may not catch

func TestCommitOffsets_OffsetZero_IncrementedToOne(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	var committedOffsets []kafka.TopicPartition
	adapter.commitOffsetsFn = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		committedOffsets = offsets
		return offsets, nil
	}

	topic := testTopicName
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0, // edge case: offset at zero
		},
	}

	msg := &nexus.Message[*kafka.Message]{
		Partition: 0,
		Offset:    0,
		Payload:   &kafkaMsg,
	}

	_, err := adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(committedOffsets) != 1 {
		t.Fatalf("expected 1 committed offset, got %d", len(committedOffsets))
	}
	// offset 0 should become 1 (next offset to read)
	if committedOffsets[0].Offset != 1 {
		t.Errorf("expected committed offset 1, got %d", committedOffsets[0].Offset)
	}
}

func TestCommitOffsets_LargeOffset_IncrementWorks(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	var committedOffsets []kafka.TopicPartition
	adapter.commitOffsetsFn = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		committedOffsets = offsets
		return offsets, nil
	}

	topic := testTopicName
	largeOffset := kafka.Offset(9223372036854775806) // MaxInt64 - 1
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    largeOffset,
		},
	}

	msg := &nexus.Message[*kafka.Message]{
		Partition: 0,
		Offset:    int64(largeOffset),
		Payload:   &kafkaMsg,
	}

	_, err := adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// large offset should increment correctly
	expectedOffset := largeOffset + 1
	if committedOffsets[0].Offset != expectedOffset {
		t.Errorf("expected committed offset %d, got %d", expectedOffset, committedOffsets[0].Offset)
	}
}

func TestCommitOffsets_OriginalMessageNotMutated(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	adapter.commitOffsetsFn = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		return offsets, nil
	}

	topic := testTopicName
	originalOffset := kafka.Offset(500)
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 3,
			Offset:    originalOffset,
		},
	}

	msg := &nexus.Message[*kafka.Message]{
		Partition: 3,
		Offset:    int64(originalOffset),
		Payload:   &kafkaMsg,
	}

	_, _ = adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})

	// verify original kafka message is NOT mutated
	if kafkaMsg.TopicPartition.Offset != originalOffset {
		t.Errorf("original message was mutated: expected offset %d, got %d",
			originalOffset, kafkaMsg.TopicPartition.Offset)
	}
}

func TestCommitOffsets_TopicAndPartitionPreserved(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	var committedOffsets []kafka.TopicPartition
	adapter.commitOffsetsFn = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		committedOffsets = offsets
		return offsets, nil
	}

	topic := "my-special-topic"
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 7,
			Offset:    42,
		},
	}

	msg := &nexus.Message[*kafka.Message]{
		Partition: 7,
		Offset:    42,
		Payload:   &kafkaMsg,
	}

	_, _ = adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})

	// verify topic and partition are preserved
	if *committedOffsets[0].Topic != "my-special-topic" {
		t.Errorf("topic not preserved: expected 'my-special-topic', got '%s'", *committedOffsets[0].Topic)
	}
	if committedOffsets[0].Partition != 7 {
		t.Errorf("partition not preserved: expected 7, got %d", committedOffsets[0].Partition)
	}
	// offset should still be incremented
	if committedOffsets[0].Offset != 43 {
		t.Errorf("offset not incremented: expected 43, got %d", committedOffsets[0].Offset)
	}
}

func TestCommitOffsets_MultiplePartitions_AllIncremented(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	var committedOffsets []kafka.TopicPartition
	adapter.commitOffsetsFn = func(offsets []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		committedOffsets = offsets
		return offsets, nil
	}

	topic := testTopicName
	messages := make([]*nexus.Message[*kafka.Message], 4)
	for i := range 4 {
		partition := int32(i) //nolint:gosec // bounded test loop
		offset := kafka.Offset(i * 1000)
		kafkaMsg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: partition,
				Offset:    offset,
			},
		}
		messages[i] = &nexus.Message[*kafka.Message]{
			Partition: partition,
			Offset:    int64(offset),
			Payload:   &kafkaMsg,
		}
	}

	_, _ = adapter.CommitOffsets(messages)

	// verify each offset is incremented independently
	expectedOffsets := []kafka.Offset{1, 1001, 2001, 3001}
	for i, tp := range committedOffsets {
		if tp.Offset != expectedOffsets[i] {
			t.Errorf("partition %d: expected offset %d, got %d", i, expectedOffsets[i], tp.Offset)
		}
		if tp.Partition != int32(i) { //nolint:gosec // bounded test loop
			t.Errorf("partition %d: partition number not preserved, got %d", i, tp.Partition)
		}
	}
}

// --- ExtractEnvelope Edge Cases ---

func TestExtractEnvelope_LargeOffset(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	largeOffset := kafka.Offset(9223372036854775807) // MaxInt64
	msg := &kafka.Message{
		Key: []byte("key"),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    largeOffset,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	// verify large offset is preserved correctly in int64 conversion
	if envelope.Offset != int64(largeOffset) {
		t.Errorf("large offset not preserved: expected %d, got %d", largeOffset, envelope.Offset)
	}
}

func TestExtractEnvelope_ZeroOffset(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	msg := &kafka.Message{
		Key: []byte("key"),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	if envelope.Offset != 0 {
		t.Errorf("zero offset not preserved: got %d", envelope.Offset)
	}
}

func TestExtractEnvelope_NegativeOffset(t *testing.T) {
	// kafka.Offset can be negative for special values like kafka.OffsetBeginning (-2)
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	msg := &kafka.Message{
		Key: []byte("key"),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    kafka.OffsetBeginning, // -2
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	// negative offsets should be preserved (though unusual for consumed messages)
	if envelope.Offset != int64(kafka.OffsetBeginning) {
		t.Errorf("negative offset not preserved: expected %d, got %d",
			kafka.OffsetBeginning, envelope.Offset)
	}
}

func TestExtractEnvelope_HighPartition(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	msg := &kafka.Message{
		Key: []byte("key"),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 1000, // high partition number
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	if envelope.Partition != 1000 {
		t.Errorf("high partition not preserved: expected 1000, got %d", envelope.Partition)
	}
}

func TestExtractEnvelope_KeyWithNullBytes(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	// key with embedded null bytes - should be treated as binary
	keyWithNull := []byte("key\x00with\x00nulls")
	msg := &kafka.Message{
		Key: keyWithNull,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	// key with null bytes is still valid UTF-8, should be used directly
	if envelope.Key != string(keyWithNull) {
		t.Errorf("key with nulls not handled correctly: got '%s'", envelope.Key)
	}
}

func TestExtractEnvelope_VeryLongKey(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()

	topic := testTopicName
	// 1MB key (unusual but valid)
	longKey := make([]byte, 1024*1024)
	for i := range longKey {
		longKey[i] = byte('a' + (i % 26))
	}
	msg := &kafka.Message{
		Key: longKey,
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: 0,
			Offset:    0,
		},
	}

	envelope := adapter.ExtractEnvelope(msg)

	// verify long key is preserved
	if len(envelope.Key) != len(longKey) {
		t.Errorf("long key length not preserved: expected %d, got %d",
			len(longKey), len(envelope.Key))
	}
}
