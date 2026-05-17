// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"context"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

const testTopicName = "test-topic"

// --- Test Helpers ---

type mockLogger struct{}

func (m *mockLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (m *mockLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (m *mockLogger) Warn(_ context.Context, _ string, _ ...any)  {}
func (m *mockLogger) Error(_ context.Context, _ string, _ ...any) {}

type mockAdaptedConsumer struct {
	triggerRebalanceCalled bool
	lastRebalanceType      nexus.RebalanceType
	lastRebalanceInfo      []nexus.RebalanceInfo
	triggerRebalanceErr    error
	ctx                    context.Context
	logger                 nexus.Logger
}

func (m *mockAdaptedConsumer) Subscribe() error         { return nil }
func (m *mockAdaptedConsumer) Shutdown() error          { return nil }
func (m *mockAdaptedConsumer) TopicName() string        { return "test-topic" }
func (m *mockAdaptedConsumer) Context() context.Context { return m.ctx }
func (m *mockAdaptedConsumer) Logger() nexus.Logger     { return m.logger }
func (m *mockAdaptedConsumer) TriggerRebalance(rt nexus.RebalanceType, info []nexus.RebalanceInfo) error {
	m.triggerRebalanceCalled = true
	m.lastRebalanceType = rt
	m.lastRebalanceInfo = info
	return m.triggerRebalanceErr
}

type mockConsumerBuilder struct {
	topicName       string
	buildCalled     bool
	adaptedConsumer nexus.AdaptedConsumer[*kafka.Message]
	ctx             context.Context
	logger          nexus.Logger
}

func (m *mockConsumerBuilder) TopicName() string {
	return m.topicName
}

func (m *mockConsumerBuilder) Build(_ nexus.BrokerPort[*kafka.Message]) nexus.AdaptedConsumer[*kafka.Message] {
	m.buildCalled = true
	return m.adaptedConsumer
}

// --- New Tests ---

func TestNew_Success(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-group",
	}
	adapter, err := New(configMap)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if adapter == nil {
		t.Fatal("expected adapter, got nil")
	}
	defer func() { _ = adapter.kafkaConsumer.Close() }()

	if adapter.kafkaConsumer == nil {
		t.Error("expected kafkaConsumer to be set")
	}
	if adapter.groupID != "test-group" {
		t.Errorf("expected groupID 'test-group', got %q", adapter.groupID)
	}
	if adapter.rebalancePolicy != FromRebalanceCallback {
		t.Errorf("expected default policy, got %v", adapter.rebalancePolicy)
	}
}

func TestNew_WithExplicitPolicy(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-group",
	}
	adapter, err := New(configMap, FromPoll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer func() { _ = adapter.kafkaConsumer.Close() }()

	if adapter.rebalancePolicy != FromPoll {
		t.Errorf("expected FromPoll, got %v", adapter.rebalancePolicy)
	}
}

func TestNew_KafkaConsumerError(t *testing.T) {
	// An unknown librdkafka config key triggers kafka.NewConsumer failure.
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":            "localhost:9092",
		"group.id":                     "test-group",
		"this.is.not.a.real.kafka.key": "value",
	}
	adapter, err := New(configMap)
	if err == nil {
		if adapter != nil {
			_ = adapter.kafkaConsumer.Close()
		}
		t.Fatal("expected error for unknown config key, got nil")
	}
	if adapter != nil {
		t.Error("expected nil adapter on error")
	}
}

// --- NewCustom Tests ---

func TestNewCustom_DefaultPolicy(t *testing.T) {
	adapter := NewCustom()

	if adapter == nil {
		t.Fatal("expected adapter, got nil")
	}
	if adapter.rebalancePolicy != FromRebalanceCallback {
		t.Errorf("expected policy %v, got %v", FromRebalanceCallback, adapter.rebalancePolicy)
	}
}

func TestNewCustom_WithExplicitPolicy(t *testing.T) {
	tests := []struct {
		name     string
		policy   RebalancePolicy
		expected RebalancePolicy
	}{
		{"FromRebalanceCallback", FromRebalanceCallback, FromRebalanceCallback},
		{"FromPoll", FromPoll, FromPoll},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := NewCustom(tt.policy)
			if adapter.rebalancePolicy != tt.expected {
				t.Errorf("expected policy %v, got %v", tt.expected, adapter.rebalancePolicy)
			}
		})
	}
}

func TestNewCustom_NilFunctionFields(t *testing.T) {
	adapter := NewCustom()

	// all function fields should be nil before SetConsumer
	if adapter.isClosedFn != nil {
		t.Error("expected isClosedFn to be nil")
	}
	if adapter.pollFn != nil {
		t.Error("expected pollFn to be nil")
	}
	if adapter.assignFn != nil {
		t.Error("expected assignFn to be nil")
	}
	if adapter.unassignFn != nil {
		t.Error("expected unassignFn to be nil")
	}
	if adapter.subscribeFn != nil {
		t.Error("expected subscribeFn to be nil")
	}
	if adapter.unsubscribeFn != nil {
		t.Error("expected unsubscribeFn to be nil")
	}
	if adapter.commitOffsetsFn != nil {
		t.Error("expected commitOffsetsFn to be nil")
	}
}

// --- SetConsumer Tests ---

func TestSetConsumer_WiresAllFunctionFields(t *testing.T) {
	adapter := NewCustom()

	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-group",
	}

	// validate.ConfigMap is invoked by New(); for NewCustom + SetConsumer the
	// caller is expected to have produced a valid configmap themselves.
	kafkaConsumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		t.Fatalf("failed to create kafka consumer: %v", err)
	}
	defer func() { _ = kafkaConsumer.Close() }()

	adapter.SetConsumer(kafkaConsumer)

	if adapter.kafkaConsumer != kafkaConsumer {
		t.Error("expected kafkaConsumer to be wired")
	}
	if adapter.isClosedFn == nil {
		t.Error("expected isClosedFn to be wired")
	}
	if adapter.pollFn == nil {
		t.Error("expected pollFn to be wired")
	}
	if adapter.assignFn == nil {
		t.Error("expected assignFn to be wired")
	}
	if adapter.unassignFn == nil {
		t.Error("expected unassignFn to be wired")
	}
	if adapter.subscribeFn == nil {
		t.Error("expected subscribeFn to be wired")
	}
	if adapter.unsubscribeFn == nil {
		t.Error("expected unsubscribeFn to be wired")
	}
	if adapter.commitOffsetsFn == nil {
		t.Error("expected commitOffsetsFn to be wired")
	}
	if adapter.incrementalAssignFn == nil {
		t.Error("expected incrementalAssignFn to be wired")
	}
	if adapter.incrementalUnassignFn == nil {
		t.Error("expected incrementalUnassignFn to be wired")
	}
	if adapter.getRebalanceProtocolFn == nil {
		t.Error("expected getRebalanceProtocolFn to be wired")
	}
	if adapter.closeFn == nil {
		t.Error("expected closeFn to be wired")
	}
	if adapter.setOAuthBearerTokenFn == nil {
		t.Error("expected setOAuthBearerTokenFn to be wired")
	}
	if adapter.setOAuthBearerTokenFailureFn == nil {
		t.Error("expected setOAuthBearerTokenFailureFn to be wired")
	}
}

// --- extractGroupID Tests ---

func TestExtractGroupID_Set(t *testing.T) {
	configMap := &kafka.ConfigMap{"group.id": "my-group"}
	if got := extractGroupID(configMap); got != "my-group" {
		t.Errorf("expected 'my-group', got %q", got)
	}
}

func TestExtractGroupID_Missing(t *testing.T) {
	configMap := &kafka.ConfigMap{}
	if got := extractGroupID(configMap); got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

func TestExtractGroupID_NotAString(t *testing.T) {
	// kafka.ConfigMap stores ConfigValue (interface{}); we can put a non-string
	// directly. extractGroupID's type assertion should fall through to "".
	configMap := &kafka.ConfigMap{}
	(*configMap)["group.id"] = 12345
	if got := extractGroupID(configMap); got != "" {
		t.Errorf("expected empty string for non-string value, got %q", got)
	}
}

// --- CreateConsumer Tests ---

func TestCreateConsumer_WiresBuilder(t *testing.T) {
	adapter := NewCustom()

	mockCtx := context.Background()
	mockLog := &mockLogger{}
	mockConsumer := &mockAdaptedConsumer{ctx: mockCtx, logger: mockLog}

	builder := &mockConsumerBuilder{
		topicName:       "test-topic",
		adaptedConsumer: mockConsumer,
	}

	consumer := adapter.CreateConsumer(builder)

	if !builder.buildCalled {
		t.Error("expected Build() to be called")
	}
	if adapter.topicName != "test-topic" {
		t.Errorf("expected topicName 'test-topic', got '%s'", adapter.topicName)
	}
	if adapter.adaptedConsumer != mockConsumer {
		t.Error("expected adaptedConsumer to be set")
	}
	if adapter.ctx != mockCtx {
		t.Error("expected ctx to be set")
	}
	if adapter.logger != mockLog {
		t.Error("expected logger to be set")
	}
	if consumer != mockConsumer {
		t.Error("expected returned consumer to be the adapted consumer")
	}
}

// --- SetOAuthTokenRefresh Tests ---

func TestSetOAuthTokenRefresh_SetsCallback(t *testing.T) {
	adapter := NewCustom()

	if adapter.oauthTokenRefreshFn != nil {
		t.Error("expected oauthTokenRefreshFn to be nil initially")
	}

	called := false
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		called = true
		return kafka.OAuthBearerToken{}, nil
	})

	if adapter.oauthTokenRefreshFn == nil {
		t.Error("expected oauthTokenRefreshFn to be set")
	}

	// verify callback works
	_, _ = adapter.oauthTokenRefreshFn()
	if !called {
		t.Error("expected callback to be invoked")
	}
}

// --- detectRebalancePolicy Tests ---

func TestDetectRebalancePolicy_NoConfig(t *testing.T) {
	configMap := &kafka.ConfigMap{}
	result := detectRebalancePolicy(configMap, FromRebalanceCallback)

	if result != FromRebalanceCallback {
		t.Errorf("expected FromRebalanceCallback, got %v", result)
	}
}

func TestDetectRebalancePolicy_EnabledBool(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"go.application.rebalance.enable": true,
	}
	result := detectRebalancePolicy(configMap, FromRebalanceCallback)

	if result != FromPoll {
		t.Errorf("expected FromPoll when enabled=true, got %v", result)
	}
}

func TestDetectRebalancePolicy_DisabledBool(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"go.application.rebalance.enable": false,
	}
	result := detectRebalancePolicy(configMap, FromRebalanceCallback)

	if result != FromRebalanceCallback {
		t.Errorf("expected FromRebalanceCallback when enabled=false, got %v", result)
	}
}

func TestDetectRebalancePolicy_EnabledString(t *testing.T) {
	// note: confluent-kafka-go ConfigMap with default value in Get() may not
	// return string types as expected. In practice, users should use bool values.
	// This test documents actual behavior where string "true" is not detected.
	configMap := &kafka.ConfigMap{
		"go.application.rebalance.enable": "true",
	}
	result := detectRebalancePolicy(configMap, FromRebalanceCallback)

	// ConfigMap.Get with bool default doesn't return string "true" as string type,
	// so the detection falls back to default policy. Users should use bool true.
	if result != FromRebalanceCallback {
		t.Errorf("expected FromRebalanceCallback (string not detected as bool), got %v", result)
	}
}

func TestDetectRebalancePolicy_DisabledString(t *testing.T) {
	// note: same as above - string values may not be detected correctly.
	// Users should use bool values for this config.
	configMap := &kafka.ConfigMap{
		"go.application.rebalance.enable": "false",
	}
	result := detectRebalancePolicy(configMap, FromRebalanceCallback)

	if result != FromRebalanceCallback {
		t.Errorf("expected FromRebalanceCallback when enabled='false', got %v", result)
	}
}

func TestDetectRebalancePolicy_PreservesExplicitPolicy(t *testing.T) {
	configMap := &kafka.ConfigMap{}
	result := detectRebalancePolicy(configMap, FromPoll)

	if result != FromPoll {
		t.Errorf("expected FromPoll to be preserved, got %v", result)
	}
}

// --- RebalancePolicy Constants Tests ---

func TestRebalancePolicy_Values(t *testing.T) {
	// ensure policies have distinct non-zero values
	if FromRebalanceCallback == 0 {
		t.Error("FromRebalanceCallback should not be zero")
	}
	if FromPoll == 0 {
		t.Error("FromPoll should not be zero")
	}
	if FromRebalanceCallback == FromPoll {
		t.Error("policies should have distinct values")
	}
}

// --- ConfluentConsumer Tests ---

func TestConfluentConsumer_ReturnsNilWhenNotSet(t *testing.T) {
	adapter := NewCustom()

	if adapter.ConfluentConsumer() != nil {
		t.Error("expected nil when consumer not set")
	}
}
