// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

// Package validate provides ConfigMap validation for confluent-kafka-go consumers.
//
// The llingr adapter requires specific configuration to function correctly with
// nexus.BrokerPort semantics. This package validates those requirements at startup
// and provides clear error messages for misconfiguration.
//
// # Enforced Settings
//
// The following settings are enforced (panic on violation):
//
//   - enable.auto.commit=false: auto-commit races with explicit offset management
//   - enable.auto.offset.store=false: offsets must only be stored after processing
//   - isolation.level: read_uncommitted breaks exactly-once guarantees
//   - go.events.channel.enable=false: adapter uses Poll(), not channels
//
// # Warnings
//
// The following trigger warnings but don't prevent startup:
//
//   - session.timeout.ms < 25s: risks group ejection during graceful shutdown
//   - max.poll.interval.ms < 30s: may conflict with container termination grace periods
//
// # Usage
//
// Call ConfigMap() before creating the Kafka consumer:
//
//	configMap := &kafka.ConfigMap{
//	    "bootstrap.servers": "localhost:9092",
//	    "group.id":          "my-group",
//	}
//	validate.ConfigMap(configMap)  // panics on invalid config
//	consumer, err := kafka.NewConsumer(configMap)
package validate

import (
	"fmt"
	"log"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// ConfigMap validates settings and helps users avoid misconfiguration.
func ConfigMap(configMap *kafka.ConfigMap) {
	ensureAutoCommitDisabled(configMap)      // enable.auto.commit        must be empty or false
	ensureAutoOffsetStoreDisabled(configMap) // enable.auto.offset.store  must be empty or false
	rejectReadUncommitted(configMap)         // isolation.level           must be empty or read_committed

	// Allowed but warned
	logCooperativeStrategy(configMap)
	rejectEventsChannelMode(configMap)
	warnIfSessionTimeoutTooLow(configMap)
	warnIfMaxPollIntervalTooLow(configMap)
}

// ensureAutoCommitDisabled sets enable.auto.commit to false if absent, or panics if true.
//
// Auto-commit races with explicit offset management, risking message loss.
func ensureAutoCommitDisabled(configMap *kafka.ConfigMap) {
	const configKey = "enable.auto.commit"

	val, err := configMap.Get(configKey, nil)
	if err != nil || val == nil {
		// SetKey on a non-prefixed key is a plain map assignment in
		// confluent-kafka-go v2 and cannot fail; discarding the error is safe.
		_ = configMap.SetKey(configKey, false)
		return
	}

	if enabled, ok := val.(bool); ok && enabled {
		panic(fmt.Sprintf("%s must be false - auto-commit conflicts with explicit offset management", configKey))
	}
}

// ensureAutoOffsetStoreDisabled sets enable.auto.offset.store to false if absent, or panics if true.
//
// Auto offset store records offsets when messages are handed to the application - before processing
// completes. With concurrent processing, messages may be in-flight across multiple workers for
// extended periods. Storing offsets before processing completes risks message loss: if the
// application crashes after offset storage but before processing, those messages are skipped on
// restart.
//
// This adapter expects explicit offset management where only contiguous ranges are committed after
// processing confirms completion. This requires full control over when offsets are stored.
//
// See: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
// "It is recommended to set enable.auto.offset.store=false for long-time processing applications"
func ensureAutoOffsetStoreDisabled(configMap *kafka.ConfigMap) {
	const configKey = "enable.auto.offset.store"

	val, err := configMap.Get(configKey, nil)
	if err != nil || val == nil {
		// See note in ensureAutoCommitDisabled - SetKey cannot fail here.
		_ = configMap.SetKey(configKey, false)
		return
	}

	if enabled, ok := val.(bool); ok && enabled {
		panic(fmt.Sprintf("%s must be false - auto offset store conflicts with explicit offset "+
			"management; offsets would be stored before processing completes, risking message loss",
			configKey))
	}
}

// logCooperativeStrategy logs when a cooperative rebalancing strategy is detected.
//
// Cooperative rebalancing (e.g., cooperative-sticky) uses incremental partition assignment where
// only changing partitions are revoked/assigned. The adapter detects the negotiated protocol at
// runtime and uses the appropriate APIs:
//   - Eager: Assign()/Unassign()
//   - Cooperative: IncrementalAssign()/IncrementalUnassign()
func logCooperativeStrategy(configMap *kafka.ConfigMap) {
	const configKey = "partition.assignment.strategy"

	val, err := configMap.Get(configKey, nil)
	if err != nil || val == nil {
		return // using default (range,roundrobin) which is eager
	}

	strategy, ok := val.(string)
	if !ok {
		return
	}

	if strings.Contains(strings.ToLower(strategy), "cooperative") {
		log.Printf("kafkaadapter: %s includes cooperative strategy; "+
			"will use incremental assign/unassign APIs", configKey)
	}
}

// rejectReadUncommitted panics if isolation.level is set to read_uncommitted.
//
// read_uncommitted allows the consumer to read messages from aborted transactions, which breaks
// exactly-once processing guarantees. Applications using transactional producers expect aborted
// messages to be invisible to consumers.
//
// The default (read_committed) is correct for virtually all use cases.
func rejectReadUncommitted(configMap *kafka.ConfigMap) {
	const configKey = "isolation.level"

	val, err := configMap.Get(configKey, nil)
	if err != nil || val == nil {
		return // using default (read_committed)
	}

	level, ok := val.(string)
	if !ok {
		return
	}

	if strings.ToLower(level) == "read_uncommitted" {
		panic(fmt.Sprintf("%s=read_uncommitted allows reading aborted transaction messages; "+
			"this breaks exactly-once guarantees; use 'read_committed' (default) instead", configKey))
	}
}

// rejectEventsChannelMode panics if go.events.channel.enable is set to true.
//
// This Go-specific setting (not librdkafka) changes the consumer from Poll()-based to channel-based
// message delivery. The adapter is built around Poll() - enabling this setting disables Poll() and
// causes the consumer to silently hang.
//
// This setting is also deprecated by Confluent in favour of the Poll() API.
func rejectEventsChannelMode(configMap *kafka.ConfigMap) {
	const configKey = "go.events.channel.enable"

	val, err := configMap.Get(configKey, nil)
	if err != nil || val == nil {
		return // using default (false)
	}

	enabled, ok := val.(bool)
	if !ok {
		return
	}

	if enabled {
		panic(fmt.Sprintf("%s=true is incompatible; the adapter uses Poll() which is disabled "+
			"when events channel mode is enabled; this setting is also deprecated by Confluent",
			configKey))
	}
}

// warnIfMaxPollIntervalTooLow warns if max.poll.interval.ms is below 30 seconds.
//
// Container orchestration platforms like Kubernetes use a termination grace period (default 30s)
// to allow graceful shutdown. If max.poll.interval.ms is lower than this grace period, the
// consumer may be ejected from the group before the application finishes draining in-flight work
// during shutdown.
//
// For example: with max.poll.interval.ms=20000 and a 30s K8s grace period, a rolling update could
// trigger a rebalance before the pod completes shutdown, causing duplicate processing when the
// partition is reassigned.
func warnIfMaxPollIntervalTooLow(configMap *kafka.ConfigMap) {
	const configKey = "max.poll.interval.ms"
	const minRecommended = 30000

	val, err := configMap.Get(configKey, nil)
	if err != nil || val == nil {
		return // using default (300000ms / 5 minutes)
	}

	interval, ok := val.(int)
	if !ok {
		return
	}

	if interval < minRecommended {
		log.Printf("kafkaadapter: %s=%d is below recommended minimum of %dms; "+
			"container orchestrators typically use 30s termination grace periods - "+
			"a lower poll interval risks group ejection before graceful shutdown completes",
			configKey, interval, minRecommended)
	}
}

// warnIfSessionTimeoutTooLow warns if session.timeout.ms is below the typical drain timeout.
//
// During graceful shutdown or rebalancing, in-flight messages must complete processing and commit
// before the consumer leaves the group. If session.timeout.ms is shorter than the drain period,
// the broker may eject the consumer before commits complete, causing duplicate processing when
// partitions are reassigned.
//
// The default drain timeout is 20 seconds. Session timeout should comfortably exceed this to allow
// for drain completion plus commit round-trip time.
func warnIfSessionTimeoutTooLow(configMap *kafka.ConfigMap) {
	const configKey = "session.timeout.ms"
	const minRecommended = 25000 // 20s drain + 5s buffer for commit round-trip

	val, err := configMap.Get(configKey, nil)
	if err != nil || val == nil {
		return // using default (45000ms)
	}

	sessionMs, ok := val.(int)
	if !ok {
		return
	}

	if sessionMs < minRecommended {
		log.Printf("kafkaadapter: %s=%d is below recommended minimum of %dms; "+
			"consumer may be ejected from group before in-flight work completes during "+
			"graceful shutdown, causing duplicate processing when partitions are reassigned",
			configKey, sessionMs, minRecommended)
	}
}
