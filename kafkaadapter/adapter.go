// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

// Package kafkaadapter implements nexus.BrokerPort for confluent-kafka-go.
//
// confluent-kafka-go wraps librdkafka via CGO, providing a mature Kafka client
// with extensive configuration options.
//
// # Quick Start
//
// For most use cases, use New() which handles client configuration:
//
//	configMap := &kafka.ConfigMap{
//	    "bootstrap.servers": "localhost:9092",
//	    "group.id":          "my-consumer-group",
//	}
//
//	adapter, err := kafkaadapter.New(configMap)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// builder is a nexus.ConsumerBuilder[*kafka.Message]; topic name comes
//	// from builder.TopicName().
//	consumer := adapter.CreateConsumer(builder)
//	if err := consumer.Subscribe(); err != nil {
//	    log.Fatal(err)
//	}
//
// # Custom Client Configuration
//
// For advanced use cases with an existing *kafka.Consumer, use NewCustom():
//
//	adapter := kafkaadapter.NewCustom()
//
//	kafkaConsumer, err := kafka.NewConsumer(configMap)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	adapter.SetConsumer(kafkaConsumer)
//	consumer := adapter.CreateConsumer(builder)
package kafkaadapter

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-adapter-kafka/kafkaadapter/validate"
	"github.com/llingr/llingr-nexus/nexus"
)

// RebalancePolicy controls how partition assignment/revocation events are received.
//
// Kafka can deliver rebalance events via callback (synchronous with Subscribe) or
// as events from Poll(). Using both causes double-handling bugs - the adapter
// auto-detects ConfigMap settings to prevent this.
type RebalancePolicy int

const (
	// FromRebalanceCallback (default) handles rebalance events via the RebalanceCb
	// callback passed to Subscribe(). This is the recommended mode.
	FromRebalanceCallback RebalancePolicy = iota + 1

	// FromPoll handles rebalance events via the Poll() loop.
	// Use this mode when go.application.rebalance.enable=true in ConfigMap.
	FromPoll
)

// Adapter wraps a confluent-kafka-go client implementing nexus.BrokerPort[*kafka.Message].
//
// Create with New() for standard configuration, or NewCustom() for advanced
// use cases with an existing *kafka.Consumer.
type Adapter struct {
	kafkaConsumer *kafka.Consumer
	logger        nexus.Logger
	ctx           context.Context
	topicName     string

	// adaptedConsumer for rebalance callbacks (has TriggerRebalance)
	adaptedConsumer nexus.AdaptedConsumer[*kafka.Message]

	// rebalance configuration
	rebalancePolicy RebalancePolicy

	// OAuth token refresh callback for OAUTHBEARER authentication
	oauthTokenRefreshFn func() (kafka.OAuthBearerToken, error)

	// function fields for testability (wired from kafkaConsumer)
	isClosedFn      func() bool
	pollFn          func(int) kafka.Event
	assignFn        func([]kafka.TopicPartition) error
	unassignFn      func() error
	subscribeFn     func(string, kafka.RebalanceCb) error
	unsubscribeFn   func() error
	commitOffsetsFn func([]kafka.TopicPartition) ([]kafka.TopicPartition, error)

	// cooperative rebalancing support - see protocol.go for RebalanceProtocol details
	incrementalAssignFn    func([]kafka.TopicPartition) error
	incrementalUnassignFn  func([]kafka.TopicPartition) error
	getRebalanceProtocolFn func() string

	// consumer lifecycle and OAuth support
	closeFn                      func() error
	setOAuthBearerTokenFn        func(kafka.OAuthBearerToken) error
	setOAuthBearerTokenFailureFn func(string) error

	// consumer group identity
	groupID string

	// bandwidth telemetry (nil when not configured)
	bwCollector *bandwidthCollector

	// configMap is retained for diagnostic logging (e.g. surfacing the configured
	// session.timeout.ms when a commit is rejected with "Unknown member"). nil for
	// adapters created via NewCustom() since the user owns the config there.
	configMap *kafka.ConfigMap
}

// New creates a confluent-kafka-go adapter with a properly configured consumer.
//
// The ConfigMap must include at minimum:
//   - bootstrap.servers: Kafka broker addresses
//   - group.id: Consumer group identifier
//
// The adapter automatically:
//   - Disables auto-commit (required for nexus consumers)
//   - Validates ConfigMap settings
//   - Detects rebalance policy from go.application.rebalance.enable
//
// Topic name is provided via the builder's WithTopicName(), not here.
//
// Returns an error if the consumer cannot be created.
func New(configMap *kafka.ConfigMap, policy ...RebalancePolicy) (*Adapter, error) {
	rebalancePolicy := FromRebalanceCallback
	if len(policy) > 0 {
		rebalancePolicy = policy[0]
	}

	validate.ConfigMap(configMap)

	rebalancePolicy = detectRebalancePolicy(configMap, rebalancePolicy)

	kafkaConsumer, err := kafka.NewConsumer(configMap)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	// Extract group.id from the ConfigMap for ConsumerGroup().
	groupID := extractGroupID(configMap)

	return &Adapter{
		kafkaConsumer:                kafkaConsumer,
		rebalancePolicy:              rebalancePolicy,
		groupID:                      groupID,
		configMap:                    configMap,
		isClosedFn:                   kafkaConsumer.IsClosed,
		pollFn:                       kafkaConsumer.Poll,
		assignFn:                     kafkaConsumer.Assign,
		unassignFn:                   kafkaConsumer.Unassign,
		subscribeFn:                  kafkaConsumer.Subscribe,
		unsubscribeFn:                kafkaConsumer.Unsubscribe,
		commitOffsetsFn:              kafkaConsumer.CommitOffsets,
		incrementalAssignFn:          kafkaConsumer.IncrementalAssign,
		incrementalUnassignFn:        kafkaConsumer.IncrementalUnassign,
		getRebalanceProtocolFn:       kafkaConsumer.GetRebalanceProtocol,
		closeFn:                      kafkaConsumer.Close,
		setOAuthBearerTokenFn:        kafkaConsumer.SetOAuthBearerToken,
		setOAuthBearerTokenFailureFn: kafkaConsumer.SetOAuthBearerTokenFailure,
	}, nil
}

// NewCustom creates a kafka adapter for use with an existing *kafka.Consumer.
//
// This is for advanced use cases where you need to configure the consumer
// yourself (e.g., custom authentication, specific librdkafka settings).
//
// After creating the adapter, call SetConsumer() to wire up the consumer,
// then CreateConsumer() to complete setup.
func NewCustom(policy ...RebalancePolicy) *Adapter {
	rebalancePolicy := FromRebalanceCallback
	if len(policy) > 0 {
		rebalancePolicy = policy[0]
	}

	return &Adapter{
		rebalancePolicy: rebalancePolicy,
	}
}

// SetConsumer attaches a *kafka.Consumer to an adapter created with NewCustom.
//
// This method wires up the function fields used by the adapter. Call this
// before CreateConsumer(). Topic name is provided via builder.WithTopicName().
func (a *Adapter) SetConsumer(kafkaConsumer *kafka.Consumer) {
	a.kafkaConsumer = kafkaConsumer
	a.isClosedFn = kafkaConsumer.IsClosed
	a.pollFn = kafkaConsumer.Poll
	a.assignFn = kafkaConsumer.Assign
	a.unassignFn = kafkaConsumer.Unassign
	a.subscribeFn = kafkaConsumer.Subscribe
	a.unsubscribeFn = kafkaConsumer.Unsubscribe
	a.commitOffsetsFn = kafkaConsumer.CommitOffsets
	a.incrementalAssignFn = kafkaConsumer.IncrementalAssign
	a.incrementalUnassignFn = kafkaConsumer.IncrementalUnassign
	a.getRebalanceProtocolFn = kafkaConsumer.GetRebalanceProtocol
	a.closeFn = kafkaConsumer.Close
	a.setOAuthBearerTokenFn = kafkaConsumer.SetOAuthBearerToken
	a.setOAuthBearerTokenFailureFn = kafkaConsumer.SetOAuthBearerTokenFailure
}

// CreateConsumer wires the builder to this adapter via Port-Binding Builder pattern.
//
// The builder carries application dependencies (processMessage, deadLetter, etc.).
// This method injects the adapter as BrokerPort. Topic name is obtained from
// builder.TopicName().
//
// Returns Consumer (not AdaptedConsumer) to hide adapter-internal methods
// like TriggerRebalance from the host application.
func (a *Adapter) CreateConsumer(builder nexus.ConsumerBuilder[*kafka.Message]) nexus.Consumer[*kafka.Message] {
	a.topicName = builder.TopicName()
	if a.bwCollector != nil {
		a.bwCollector.topicName = a.topicName
	}
	a.adaptedConsumer = builder.Build(a)
	a.ctx = a.adaptedConsumer.Context()
	a.logger = a.adaptedConsumer.Logger()
	return a.adaptedConsumer
}

// ConfluentConsumer exposes the underlying confluent-kafka-go Consumer for
// advanced use cases such as querying metadata or other client-specific operations.
func (a *Adapter) ConfluentConsumer() *kafka.Consumer {
	return a.kafkaConsumer
}

// SetOAuthTokenRefresh registers a callback for OAUTHBEARER token refresh.
//
// When using SASL/OAUTHBEARER authentication, librdkafka requests new tokens
// via OAuthBearerTokenRefresh events (on startup and at 80% of token lifetime).
// This callback is invoked to fetch a fresh token.
//
// The callback should return a valid OAuthBearerToken or an error. On error,
// the adapter calls SetOAuthBearerTokenFailure() which schedules a retry in 10s.
//
// Example:
//
//	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
//	    token, err := fetchTokenFromIDP()
//	    if err != nil {
//	        return kafka.OAuthBearerToken{}, err
//	    }
//	    return kafka.OAuthBearerToken{
//	        TokenValue: token.AccessToken,
//	        Expiration: token.Expiry,
//	        Principal:  "user@example.com",
//	    }, nil
//	})
func (a *Adapter) SetOAuthTokenRefresh(fn func() (kafka.OAuthBearerToken, error)) {
	a.oauthTokenRefreshFn = fn
}

// WithBandwidthInterval enables bandwidth telemetry collection at the given cadence.
// The adapter parses librdkafka statistics JSON and emits BandwidthMetrics packets
// via the callback registered by the framework.
//
// Must match statistics.interval.ms in the ConfigMap; the adapter does not
// modify the ConfigMap. Without that setting librdkafka emits no stats events
// and no bandwidth packets will flow.
//
// Must be called before CreateConsumer().
//
// Valid range: 1s to 12h. Zero uses the default (1 minute).
func (a *Adapter) WithBandwidthInterval(d time.Duration) *Adapter {
	if err := nexus.ValidateBandwidthInterval(d); err != nil {
		panic(fmt.Errorf("WithBandwidthInterval: %w", err))
	}
	interval := d
	if interval == 0 {
		interval = nexus.DefaultBandwidthInterval
	}
	a.bwCollector = &bandwidthCollector{
		interval: interval,
	}
	return a
}

// extractGroupID retrieves the "group.id" value from a ConfigMap.
// Returns "" if not set or not a string. Uses direct map access (kafka.ConfigMap
// is a map[string]ConfigValue) so a comma-ok type assertion handles all three
// cases - absent, string, or non-string - in one expression.
func extractGroupID(conf *kafka.ConfigMap) string {
	s, _ := (*conf)["group.id"].(string)
	return s
}

// detectRebalancePolicy checks the ConfigMap for 'go.application.rebalance.enable'
// and determines the appropriate RebalancePolicy. The setting is treated as bool;
// non-bool values (including string "true") fall back to the supplied policy.
// Direct map access is used instead of ConfigMap.Get to avoid the type-mismatch
// error path that's equivalent in effect to falling back.
func detectRebalancePolicy(conf *kafka.ConfigMap, policy RebalancePolicy) RebalancePolicy {
	v, _ := (*conf)["go.application.rebalance.enable"].(bool)
	if v {
		return FromPoll
	}
	return policy
}
