// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"context"
	"errors"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

const (
	protocolEager       = "EAGER"
	protocolCooperative = "COOPERATIVE"
)

// --- handleRebalanceEvent Tests ---

func TestHandleRebalanceEvent_AssignedPartitions(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.assignFn = func(_ []kafka.TopicPartition) error { return nil }

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 0, Offset: 100},
		},
	}

	err := adapter.handleRebalanceEvent(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance to be called")
	}
	if mockConsumer.lastRebalanceType != nexus.Assign {
		t.Errorf("expected Assign, got %v", mockConsumer.lastRebalanceType)
	}
}

func TestHandleRebalanceEvent_RevokedPartitions(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.unassignFn = func() error { return nil }

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 0, Offset: 100},
		},
	}

	err := adapter.handleRebalanceEvent(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance to be called")
	}
	if mockConsumer.lastRebalanceType != nexus.Revoke {
		t.Errorf("expected Revoke, got %v", mockConsumer.lastRebalanceType)
	}
}

func TestHandleRebalanceEvent_UnknownEvent(t *testing.T) {
	adapter := NewCustom()

	// use a different event type
	event := kafka.OffsetsCommitted{}

	err := adapter.handleRebalanceEvent(event)

	if err != nil {
		t.Errorf("unexpected error for unknown event: %v", err)
	}
}

// --- handleAssigned Tests ---

func TestHandleAssigned_NilAdaptedConsumer(t *testing.T) {
	adapter := NewCustom()
	adapter.adaptedConsumer = nil

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandleAssigned_BuildsCorrectInfo(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = "orders-topic"

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.assignFn = func(_ []kafka.TopicPartition) error { return nil }

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 0, Offset: 100},
			{Partition: 1, Offset: 200},
			{Partition: 2, Offset: 300},
		},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockConsumer.lastRebalanceInfo) != 3 {
		t.Fatalf("expected 3 rebalance info entries, got %d", len(mockConsumer.lastRebalanceInfo))
	}

	for i, info := range mockConsumer.lastRebalanceInfo {
		if info.RebalanceType != nexus.Assign {
			t.Errorf("info[%d]: expected Assign, got %v", i, info.RebalanceType)
		}
		if info.TopicName != "orders-topic" {
			t.Errorf("info[%d]: expected topic 'orders-topic', got '%s'", i, info.TopicName)
		}
		partition := int32(i) // #nosec G115 -- test code with small loop values
		if info.Partition != partition {
			t.Errorf("info[%d]: expected partition %d, got %d", i, i, info.Partition)
		}
		expectedOffset := int64((i + 1) * 100)
		if info.CommittedOffset != expectedOffset {
			t.Errorf("info[%d]: expected offset %d, got %d", i, expectedOffset, info.CommittedOffset)
		}
	}
}

func TestHandleAssigned_TriggerRebalanceError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	expectedErr := errors.New("trigger failed")
	mockConsumer := &mockAdaptedConsumer{
		triggerRebalanceErr: expectedErr,
	}
	adapter.adaptedConsumer = mockConsumer

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleAssigned(event)

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestHandleAssigned_AssignError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	expectedErr := errors.New("assign failed")
	adapter.assignFn = func(_ []kafka.TopicPartition) error {
		return expectedErr
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleAssigned(event)

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestHandleAssigned_EmptyPartitions(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	assignCalled := false
	adapter.assignFn = func(partitions []kafka.TopicPartition) error {
		assignCalled = true
		if len(partitions) != 0 {
			t.Errorf("expected empty partitions, got %d", len(partitions))
		}
		return nil
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !mockConsumer.triggerRebalanceCalled {
		t.Error("expected TriggerRebalance to be called even with empty partitions")
	}
	if !assignCalled {
		t.Error("expected assignFn to be called")
	}
}

// --- handleRevoked Tests ---

func TestHandleRevoked_NilAdaptedConsumer(t *testing.T) {
	adapter := NewCustom()
	adapter.adaptedConsumer = nil

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleRevoked(event)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestHandleRevoked_BuildsCorrectInfo(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = "events-topic"

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.unassignFn = func() error { return nil }

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 5, Offset: 500},
			{Partition: 6, Offset: 600},
		},
	}

	err := adapter.handleRevoked(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockConsumer.lastRebalanceInfo) != 2 {
		t.Fatalf("expected 2 rebalance info entries, got %d", len(mockConsumer.lastRebalanceInfo))
	}

	expectedPartitions := []int32{5, 6}
	expectedOffsets := []int64{500, 600}

	for i, info := range mockConsumer.lastRebalanceInfo {
		if info.RebalanceType != nexus.Revoke {
			t.Errorf("info[%d]: expected Revoke, got %v", i, info.RebalanceType)
		}
		if info.TopicName != "events-topic" {
			t.Errorf("info[%d]: expected topic 'events-topic', got '%s'", i, info.TopicName)
		}
		if info.Partition != expectedPartitions[i] {
			t.Errorf("info[%d]: expected partition %d, got %d", i, expectedPartitions[i], info.Partition)
		}
		if info.CommittedOffset != expectedOffsets[i] {
			t.Errorf("info[%d]: expected offset %d, got %d", i, expectedOffsets[i], info.CommittedOffset)
		}
	}
}

func TestHandleRevoked_TriggerRebalanceError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	expectedErr := errors.New("trigger failed")
	mockConsumer := &mockAdaptedConsumer{
		triggerRebalanceErr: expectedErr,
	}
	adapter.adaptedConsumer = mockConsumer

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleRevoked(event)

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestHandleRevoked_UnassignError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer

	expectedErr := errors.New("unassign failed")
	adapter.unassignFn = func() error {
		return expectedErr
	}

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleRevoked(event)

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestHandleRevoked_CallsUnassignAfterTrigger(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	callOrder := []string{}

	mockConsumer := &mockAdaptedConsumer{}
	// override TriggerRebalance via the mock
	originalTrigger := mockConsumer.TriggerRebalance
	_ = originalTrigger // suppress unused warning

	adapter.adaptedConsumer = &orderTrackingConsumer{
		onTrigger: func() {
			callOrder = append(callOrder, "trigger")
		},
	}

	adapter.unassignFn = func() error {
		callOrder = append(callOrder, "unassign")
		return nil
	}

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	_ = adapter.handleRevoked(event)

	if len(callOrder) != 2 {
		t.Fatalf("expected 2 calls, got %d: %v", len(callOrder), callOrder)
	}
	if callOrder[0] != "trigger" {
		t.Errorf("expected 'trigger' first, got '%s'", callOrder[0])
	}
	if callOrder[1] != "unassign" {
		t.Errorf("expected 'unassign' second, got '%s'", callOrder[1])
	}
}

// helper for tracking call order
type orderTrackingConsumer struct {
	onTrigger func()
}

func (o *orderTrackingConsumer) Subscribe() error         { return nil }
func (o *orderTrackingConsumer) Shutdown() error          { return nil }
func (o *orderTrackingConsumer) TopicName() string        { return testTopicName }
func (o *orderTrackingConsumer) Context() context.Context { return context.Background() }
func (o *orderTrackingConsumer) Logger() nexus.Logger     { return &mockLogger{} }
func (o *orderTrackingConsumer) TriggerRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	if o.onTrigger != nil {
		o.onTrigger()
	}
	return nil
}

// --- getRebalanceProtocol Tests ---

func TestGetRebalanceProtocol_NilFn(t *testing.T) {
	adapter := NewCustom()
	adapter.getRebalanceProtocolFn = nil

	protocol := adapter.getRebalanceProtocol()

	if protocol != ProtocolEager {
		t.Errorf("expected ProtocolEager when fn is nil, got %v", protocol)
	}
}

func TestGetRebalanceProtocol_ReturnsEager(t *testing.T) {
	adapter := NewCustom()
	adapter.getRebalanceProtocolFn = func() string { return protocolEager }

	protocol := adapter.getRebalanceProtocol()

	if protocol != ProtocolEager {
		t.Errorf("expected ProtocolEager, got %v", protocol)
	}
}

func TestGetRebalanceProtocol_ReturnsCooperative(t *testing.T) {
	adapter := NewCustom()
	adapter.getRebalanceProtocolFn = func() string { return protocolCooperative }

	protocol := adapter.getRebalanceProtocol()

	if protocol != ProtocolCooperative {
		t.Errorf("expected ProtocolCooperative, got %v", protocol)
	}
}

func TestGetRebalanceProtocol_EmptyStringFallback(t *testing.T) {
	adapter := NewCustom()
	adapter.getRebalanceProtocolFn = func() string { return "" }

	protocol := adapter.getRebalanceProtocol()

	if protocol != ProtocolEager {
		t.Errorf("expected ProtocolEager for empty string, got %v", protocol)
	}
}

// --- Cooperative Protocol Tests ---

func TestHandleAssigned_CooperativeProtocol(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolCooperative }

	incrementalAssignCalled := false
	var assignedPartitions []kafka.TopicPartition
	adapter.incrementalAssignFn = func(partitions []kafka.TopicPartition) error {
		incrementalAssignCalled = true
		assignedPartitions = partitions
		return nil
	}

	// ensure regular assignFn is NOT called
	adapter.assignFn = func(_ []kafka.TopicPartition) error {
		t.Error("assignFn should not be called for cooperative protocol")
		return nil
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 0, Offset: 100},
			{Partition: 1, Offset: 200},
		},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !incrementalAssignCalled {
		t.Error("expected incrementalAssignFn to be called")
	}
	if len(assignedPartitions) != 2 {
		t.Errorf("expected 2 partitions, got %d", len(assignedPartitions))
	}
}

func TestHandleRevoked_CooperativeProtocol(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolCooperative }

	incrementalUnassignCalled := false
	var unassignedPartitions []kafka.TopicPartition
	adapter.incrementalUnassignFn = func(partitions []kafka.TopicPartition) error {
		incrementalUnassignCalled = true
		unassignedPartitions = partitions
		return nil
	}

	// ensure regular unassignFn is NOT called
	adapter.unassignFn = func() error {
		t.Error("unassignFn should not be called for cooperative protocol")
		return nil
	}

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 2, Offset: 300},
		},
	}

	err := adapter.handleRevoked(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !incrementalUnassignCalled {
		t.Error("expected incrementalUnassignFn to be called")
	}
	if len(unassignedPartitions) != 1 {
		t.Errorf("expected 1 partition, got %d", len(unassignedPartitions))
	}
	if unassignedPartitions[0].Partition != 2 {
		t.Errorf("expected partition 2, got %d", unassignedPartitions[0].Partition)
	}
}

func TestHandleAssigned_CooperativeError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolCooperative }

	expectedErr := errors.New("incremental assign failed")
	adapter.incrementalAssignFn = func(_ []kafka.TopicPartition) error {
		return expectedErr
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleAssigned(event)

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestHandleRevoked_CooperativeError(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolCooperative }

	expectedErr := errors.New("incremental unassign failed")
	adapter.incrementalUnassignFn = func(_ []kafka.TopicPartition) error {
		return expectedErr
	}

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleRevoked(event)

	if err == nil {
		t.Error("expected error")
	}
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error to wrap %v, got %v", expectedErr, err)
	}
}

func TestHandleAssigned_EagerProtocol_UsesAssignFn(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolEager }

	assignCalled := false
	adapter.assignFn = func(_ []kafka.TopicPartition) error {
		assignCalled = true
		return nil
	}

	// ensure incrementalAssignFn is NOT called
	adapter.incrementalAssignFn = func(_ []kafka.TopicPartition) error {
		t.Error("incrementalAssignFn should not be called for eager protocol")
		return nil
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !assignCalled {
		t.Error("expected assignFn to be called for eager protocol")
	}
}

func TestHandleRevoked_EagerProtocol_UsesUnassignFn(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolEager }

	unassignCalled := false
	adapter.unassignFn = func() error {
		unassignCalled = true
		return nil
	}

	// ensure incrementalUnassignFn is NOT called
	adapter.incrementalUnassignFn = func(_ []kafka.TopicPartition) error {
		t.Error("incrementalUnassignFn should not be called for eager protocol")
		return nil
	}

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{{Partition: 0}},
	}

	err := adapter.handleRevoked(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !unassignCalled {
		t.Error("expected unassignFn to be called for eager protocol")
	}
}

func TestHandleAssigned_CooperativeEmptyPartitions(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolCooperative }

	incrementalAssignCalled := false
	adapter.incrementalAssignFn = func(partitions []kafka.TopicPartition) error {
		incrementalAssignCalled = true
		if len(partitions) != 0 {
			t.Errorf("expected empty partitions, got %d", len(partitions))
		}
		return nil
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !incrementalAssignCalled {
		t.Error("expected incrementalAssignFn to be called even with empty partitions")
	}
}

func TestHandleRevoked_CooperativeEmptyPartitions(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.getRebalanceProtocolFn = func() string { return protocolCooperative }

	incrementalUnassignCalled := false
	adapter.incrementalUnassignFn = func(partitions []kafka.TopicPartition) error {
		incrementalUnassignCalled = true
		if len(partitions) != 0 {
			t.Errorf("expected empty partitions, got %d", len(partitions))
		}
		return nil
	}

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{},
	}

	err := adapter.handleRevoked(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !incrementalUnassignCalled {
		t.Error("expected incrementalUnassignFn to be called even with empty partitions")
	}
}

// --- Additional Edge Cases ---

func TestHandleAssigned_LargeNumberOfPartitions(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.assignFn = func(_ []kafka.TopicPartition) error { return nil }

	// create 100 partitions
	partitions := make([]kafka.TopicPartition, 100)
	for i := range 100 {
		partitions[i] = kafka.TopicPartition{
			Partition: int32(i), //nolint:gosec // bounded test loop
			Offset:    kafka.Offset(i * 1000),
		}
	}

	event := kafka.AssignedPartitions{Partitions: partitions}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockConsumer.lastRebalanceInfo) != 100 {
		t.Errorf("expected 100 rebalance info entries, got %d", len(mockConsumer.lastRebalanceInfo))
	}

	// verify all partitions and offsets are preserved
	for i, info := range mockConsumer.lastRebalanceInfo {
		if info.Partition != int32(i) { //nolint:gosec // bounded test loop
			t.Errorf("partition %d: expected %d, got %d", i, i, info.Partition)
		}
		expectedOffset := int64(i * 1000)
		if info.CommittedOffset != expectedOffset {
			t.Errorf("partition %d: expected offset %d, got %d", i, expectedOffset, info.CommittedOffset)
		}
	}
}

func TestHandleAssigned_SpecialOffsetValues(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.assignFn = func(_ []kafka.TopicPartition) error { return nil }

	// test with special Kafka offset values
	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 0, Offset: kafka.OffsetBeginning}, // -2
			{Partition: 1, Offset: kafka.OffsetEnd},       // -1
			{Partition: 2, Offset: kafka.OffsetStored},    // -1000
			{Partition: 3, Offset: kafka.OffsetInvalid},   // -1001
			{Partition: 4, Offset: 0},                     // explicit zero
			{Partition: 5, Offset: 9223372036854775807},   // MaxInt64
		},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockConsumer.lastRebalanceInfo) != 6 {
		t.Fatalf("expected 6 rebalance info entries, got %d", len(mockConsumer.lastRebalanceInfo))
	}

	// verify special offsets are preserved correctly
	expectedOffsets := []int64{
		int64(kafka.OffsetBeginning),
		int64(kafka.OffsetEnd),
		int64(kafka.OffsetStored),
		int64(kafka.OffsetInvalid),
		0,
		9223372036854775807,
	}

	for i, info := range mockConsumer.lastRebalanceInfo {
		if info.CommittedOffset != expectedOffsets[i] {
			t.Errorf("partition %d: expected offset %d, got %d", i, expectedOffsets[i], info.CommittedOffset)
		}
	}
}

func TestHandleAssigned_SinglePartition(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.assignFn = func(partitions []kafka.TopicPartition) error {
		if len(partitions) != 1 {
			t.Errorf("expected 1 partition, got %d", len(partitions))
		}
		return nil
	}

	event := kafka.AssignedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 42, Offset: 12345},
		},
	}

	err := adapter.handleAssigned(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockConsumer.lastRebalanceInfo) != 1 {
		t.Fatalf("expected 1 rebalance info entry, got %d", len(mockConsumer.lastRebalanceInfo))
	}
	if mockConsumer.lastRebalanceInfo[0].Partition != 42 {
		t.Errorf("expected partition 42, got %d", mockConsumer.lastRebalanceInfo[0].Partition)
	}
}

func TestHandleRevoked_SinglePartition(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.topicName = testTopicName

	mockConsumer := &mockAdaptedConsumer{}
	adapter.adaptedConsumer = mockConsumer
	adapter.unassignFn = func() error { return nil }

	event := kafka.RevokedPartitions{
		Partitions: []kafka.TopicPartition{
			{Partition: 99, Offset: 54321},
		},
	}

	err := adapter.handleRevoked(event)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mockConsumer.lastRebalanceInfo) != 1 {
		t.Fatalf("expected 1 rebalance info entry, got %d", len(mockConsumer.lastRebalanceInfo))
	}
	if mockConsumer.lastRebalanceInfo[0].Partition != 99 {
		t.Errorf("expected partition 99, got %d", mockConsumer.lastRebalanceInfo[0].Partition)
	}
	if mockConsumer.lastRebalanceInfo[0].CommittedOffset != 54321 {
		t.Errorf("expected offset 54321, got %d", mockConsumer.lastRebalanceInfo[0].CommittedOffset)
	}
}

func TestGetRebalanceProtocol_UnknownValueFallback(t *testing.T) {
	adapter := NewCustom()
	adapter.getRebalanceProtocolFn = func() string { return "SOMETHING_NEW" }

	protocol := adapter.getRebalanceProtocol()

	// unknown values should fall back to eager (safe default)
	if protocol != ProtocolEager {
		t.Errorf("expected ProtocolEager for unknown value, got %v", protocol)
	}
}
