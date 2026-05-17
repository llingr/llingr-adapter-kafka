// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

// broker_port.go implements nexus.BrokerPort[*kafka.Message] for the Adapter.
//
// This file contains the core broker operations that consumer frameworks call:
//   - Subscribe/Unsubscribe: topic subscription lifecycle
//   - Poll: fetch messages and handle Kafka events (rebalance, errors, OAuth)
//   - ExtractEnvelope: map Kafka messages to nexus.Envelope (partition, offset, key)
//   - CommitOffsets: commit processed message offsets to the broker
//   - AckRebalance/BrokerQuery: no-op stubs (confluent-kafka-go handles internally)
//
// The adapter translates between confluent-kafka-go's event-driven model and the
// nexus.BrokerPort interface, enabling consumer frameworks to work with Kafka
// without direct dependency on the Kafka client library.

package kafkaadapter

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

// Compile-time verification of interface compliance.
var _ nexus.BrokerPort[*kafka.Message] = (*Adapter)(nil)
var _ nexus.BandwidthPort[*kafka.Message] = (*Adapter)(nil)

// Subscribe to topic with appropriate rebalance handling.
// Topic name is provided at adapter construction, not here.
//
// For FromRebalanceCallback policy (default), registers a rebalance callback.
// For FromPoll policy, rebalance events are handled in Poll() instead.
func (a *Adapter) Subscribe() error {
	a.logger.Info(a.ctx, "subscribing to topic: %s (rebalancePolicy: %v)", a.topicName, a.rebalancePolicy)

	var rebalanceCb kafka.RebalanceCb
	if a.rebalancePolicy == FromRebalanceCallback {
		rebalanceCb = func(_ *kafka.Consumer, event kafka.Event) error {
			return a.handleRebalanceEvent(event)
		}
	}

	if err := a.subscribeFn(a.topicName, rebalanceCb); err != nil {
		a.logger.Error(a.ctx, "subscribe error: %v", err)
		return err
	}

	return nil
}

// Unsubscribe and close the consumer.
func (a *Adapter) Unsubscribe() error {
	if a.closeFn != nil {
		_ = a.unsubscribeFn()
		_ = a.closeFn()
	}
	return nil
}

// Poll fetches the next message from Kafka.
//
// Handles multiple event types from confluent-kafka-go:
//   - *kafka.Message: actual message to process
//   - kafka.AssignedPartitions: partition assignment (if FromPoll policy)
//   - kafka.RevokedPartitions: partition revocation (if FromPoll policy)
//   - kafka.OAuthBearerTokenRefresh: token refresh request (if OAUTHBEARER auth)
//   - kafka.Error: broker errors
//   - kafka.OffsetsCommitted: commit confirmations (logged)
func (a *Adapter) Poll(timeout time.Duration) (*kafka.Message, bool, error) {
	if a.isClosedFn() {
		return nil, false, kafka.NewError(kafka.ErrState, "consumer closed", false)
	}

	event := a.pollFn(int(timeout.Milliseconds()))
	if event == nil {
		return nil, false, nil
	}

	switch ev := event.(type) {
	case *kafka.Message:
		if ev.TopicPartition.Error != nil {
			a.logger.Warn(a.ctx, "partition error on %s[%d]: %v",
				*ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Error)
			return ev, false, ev.TopicPartition.Error
		}
		return ev, true, nil

	case kafka.AssignedPartitions:
		if a.rebalancePolicy == FromPoll {
			return nil, false, a.handleAssigned(ev)
		}
		a.logger.Debug(a.ctx, "skipped AssignedPartitions in poll (handled by callback)")
		return nil, false, nil

	case kafka.RevokedPartitions:
		if a.rebalancePolicy == FromPoll {
			return nil, false, a.handleRevoked(ev)
		}
		a.logger.Debug(a.ctx, "skipped RevokedPartitions in poll (handled by callback)")
		return nil, false, nil

	case kafka.Error:
		return nil, false, ev

	case kafka.OffsetsCommitted:
		a.logger.Debug(a.ctx, "offsets committed: %v", ev)
		return nil, false, nil

	case kafka.OAuthBearerTokenRefresh:
		a.handleOAuthTokenRefresh()
		return nil, false, nil

	case *kafka.Stats:
		if a.bwCollector != nil {
			a.bwCollector.handleStats(ev.String())
		}
		return nil, false, nil

	default:
		a.logger.Debug(a.ctx, "ignored event type %T: %v", ev, ev)
		return nil, false, nil
	}
}

// handleOAuthTokenRefresh handles OAUTHBEARER token refresh requests from librdkafka.
func (a *Adapter) handleOAuthTokenRefresh() {
	if a.oauthTokenRefreshFn == nil {
		a.logger.Warn(a.ctx, "OAuthBearerTokenRefresh received but no handler registered; "+
			"call SetOAuthTokenRefresh()")
		if a.setOAuthBearerTokenFailureFn != nil {
			_ = a.setOAuthBearerTokenFailureFn("no token refresh handler registered")
		}
		return
	}

	token, err := a.oauthTokenRefreshFn()
	if err != nil {
		a.logger.Error(a.ctx, "OAuth token refresh failed: %v", err)
		if a.setOAuthBearerTokenFailureFn != nil {
			_ = a.setOAuthBearerTokenFailureFn(err.Error())
		}
		return
	}

	if a.setOAuthBearerTokenFn != nil {
		if err = a.setOAuthBearerTokenFn(token); err != nil {
			a.logger.Error(a.ctx, "failed to set OAuth bearer token: %v", err)
			if a.setOAuthBearerTokenFailureFn != nil {
				_ = a.setOAuthBearerTokenFailureFn(err.Error())
			}
			return
		}
	}

	a.logger.Debug(a.ctx, "OAuth bearer token refreshed successfully")
}

// ExtractEnvelope maps a *kafka.Message to nexus.Envelope.
//
// Key extraction handles all key types safely:
//   - UTF-8 string keys are used directly
//   - Binary keys are base64 encoded (safe, deterministic)
//   - Empty keys fall back to partition number
//
// For optimal performance or custom context injection (traces/spans),
// override via builder.WithExtractEnvelope().
func (a *Adapter) ExtractEnvelope(msg *kafka.Message) nexus.Envelope {
	var key string
	msgKey := msg.Key

	switch {
	case len(msgKey) > 0 && utf8.Valid(msgKey):
		key = string(msgKey)
	case len(msgKey) > 0:
		key = base64.StdEncoding.EncodeToString(msgKey)
	default:
		key = strconv.Itoa(int(msg.TopicPartition.Partition))
	}

	return nexus.Envelope{
		Partition: msg.TopicPartition.Partition,
		Offset:    int64(msg.TopicPartition.Offset),
		Key:       key,
		Ctx:       a.ctx,
	}
}

// CommitOffsets commits the specified messages to the broker.
//
// Kafka commits the "next offset to read", so offsets are incremented by 1.
func (a *Adapter) CommitOffsets(messages []*nexus.Message[*kafka.Message]) ([]*nexus.Message[*kafka.Message], error) {
	if len(messages) == 0 {
		return nil, nil
	}

	commits := make([]kafka.TopicPartition, 0, len(messages))
	for _, message := range messages {
		msg := *message.Payload
		tp := msg.TopicPartition
		tp.Offset++ // Kafka commits "next offset to read"
		commits = append(commits, tp)
	}

	out, err := a.commitOffsetsFn(commits)
	if err != nil {
		a.logger.Error(a.ctx, "failed to commit offsets: %v", err)
		a.logGroupMembershipHintIfApplicable(err)
		return messages, err
	}

	a.logger.Debug(a.ctx, "committed offsets: %v", out)
	return nil, nil
}

// logGroupMembershipHintIfApplicable surfaces an actionable advisory when a commit
// fails because the broker has already removed this consumer from the group
// (typically session.timeout.ms exceeded during drain pressure). The advice is
// terminal - retrying is pointless because group membership is already gone -
// so the operator-facing fix is to give the consumer more heartbeat slack.
func (a *Adapter) logGroupMembershipHintIfApplicable(err error) {
	if !isGroupMembershipLost(err) {
		return
	}
	a.logger.Warn(a.ctx,
		"commit rejected (group membership lost): session timeout currently %s - likely exceeded during drain, raise via session.timeout.ms config",
		a.currentSessionTimeoutDescription())
}

// isGroupMembershipLost reports whether a commit error indicates the broker
// has already removed this consumer from the group. Typed match preferred;
// string fallback handles librdkafka FFI-edge cases where the error doesn't
// surface as kafka.Error.
func isGroupMembershipLost(err error) bool {
	if err == nil {
		return false
	}
	var ke kafka.Error
	if errors.As(err, &ke) {
		switch ke.Code() {
		case kafka.ErrUnknownMemberID, kafka.ErrIllegalGeneration:
			return true
		}
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown member") || strings.Contains(msg, "illegal generation")
}

// currentSessionTimeoutDescription returns a human-readable description of the
// configured session.timeout.ms value, falling back to a default-marker string
// when the config wasn't set explicitly (librdkafka applies its own default).
func (a *Adapter) currentSessionTimeoutDescription() string {
	if a.configMap == nil {
		return "<unknown - adapter created via NewCustom>"
	}
	v, _ := a.configMap.Get("session.timeout.ms", nil)
	if v == nil {
		return "<librdkafka default ~45s>"
	}
	return fmt.Sprintf("%vms", v)
}

// AckRebalance acknowledges rebalance completion.
// confluent-kafka-go handles this internally via Assign/Unassign, so this is a no-op.
func (a *Adapter) AckRebalance(_ nexus.RebalanceType, _ []nexus.RebalanceInfo) error {
	return nil
}

// BrokerQuery for committed offsets and other queries.
// This is a no-op for confluent-kafka-go.
func (a *Adapter) BrokerQuery(_ nexus.QueryRequest) (nexus.QueryResponse, error) {
	return nexus.QueryResponse{}, nil
}

// ConsumerGroup returns the consumer group ID configured on this adapter.
// Returns "" for adapters created with NewCustom() where the group ID is unknown.
func (a *Adapter) ConsumerGroup() string {
	return a.groupID
}
