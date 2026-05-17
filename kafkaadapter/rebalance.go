// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

// rebalance.go handles Kafka consumer group rebalancing events.
//
// When Kafka rebalances partitions (consumer joins/leaves, topic changes), the
// adapter must coordinate with the consumer framework to:
//
//  1. Drain in-flight messages for revoked partitions before releasing them
//  2. Commit final offsets before partition ownership transfers
//  3. Reset state and begin processing from assigned partition offsets
//
// This coordination happens via nexus.AdaptedConsumer.TriggerRebalance(), which
// the consumer framework implements. The adapter translates Kafka's partition
// events into nexus.RebalanceInfo structs and invokes the appropriate callbacks.
//
// Rebalance events can arrive via two paths depending on RebalancePolicy:
//   - FromRebalanceCallback: synchronous callback during Subscribe() (default)
//   - FromPoll: as events returned from Poll() when go.application.rebalance.enable=true
//
// The adapter supports both eager and cooperative rebalancing protocols:
//   - Eager (default): all partitions revoked before reassignment, uses Assign()/Unassign()
//   - Cooperative: only changing partitions affected, uses IncrementalAssign()/IncrementalUnassign()
//
// See protocol.go for the RebalanceProtocol type.

package kafkaadapter

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

// handleRebalanceEvent routes rebalance events from the callback.
func (a *Adapter) handleRebalanceEvent(event kafka.Event) error {
	switch ev := event.(type) {
	case kafka.AssignedPartitions:
		return a.handleAssigned(ev)
	case kafka.RevokedPartitions:
		return a.handleRevoked(ev)
	default:
		return nil
	}
}

// handleAssigned processes partition assignment.
func (a *Adapter) handleAssigned(ev kafka.AssignedPartitions) error {
	if a.adaptedConsumer == nil {
		return nil
	}

	info := make([]nexus.RebalanceInfo, len(ev.Partitions))
	for i, tp := range ev.Partitions {
		info[i] = nexus.RebalanceInfo{
			RebalanceType:   nexus.Assign,
			TopicName:       a.topicName,
			Partition:       tp.Partition,
			CommittedOffset: int64(tp.Offset),
		}
	}

	if err := a.adaptedConsumer.TriggerRebalance(nexus.Assign, info); err != nil {
		return fmt.Errorf("failed to trigger rebalance (assign): %w", err)
	}

	protocol := a.getRebalanceProtocol()

	if protocol == ProtocolCooperative {
		a.logger.Info(a.ctx, "incrementally assigning partitions: %v", ev.Partitions)
		if err := a.incrementalAssignFn(ev.Partitions); err != nil {
			return fmt.Errorf("incremental assign failed: %w", err)
		}
	} else {
		a.logger.Info(a.ctx, "assigning partitions: %v", ev.Partitions)
		if err := a.assignFn(ev.Partitions); err != nil {
			return fmt.Errorf("assign failed: %w", err)
		}
	}

	return nil
}

// handleRevoked processes partition revocation.
func (a *Adapter) handleRevoked(ev kafka.RevokedPartitions) error {
	if a.adaptedConsumer == nil {
		return nil
	}

	info := make([]nexus.RebalanceInfo, len(ev.Partitions))
	for i, tp := range ev.Partitions {
		info[i] = nexus.RebalanceInfo{
			RebalanceType:   nexus.Revoke,
			TopicName:       a.topicName,
			Partition:       tp.Partition,
			CommittedOffset: int64(tp.Offset),
		}
	}

	if err := a.adaptedConsumer.TriggerRebalance(nexus.Revoke, info); err != nil {
		return fmt.Errorf("failed to trigger rebalance (revoke): %w", err)
	}

	protocol := a.getRebalanceProtocol()

	if protocol == ProtocolCooperative {
		a.logger.Info(a.ctx, "incrementally unassigning partitions: %v", ev.Partitions)
		if err := a.incrementalUnassignFn(ev.Partitions); err != nil {
			return fmt.Errorf("incremental unassign failed: %w", err)
		}
	} else {
		a.logger.Info(a.ctx, "unassigning partitions: %v", ev.Partitions)
		if err := a.unassignFn(); err != nil {
			return fmt.Errorf("unassign failed: %w", err)
		}
	}

	return nil
}

// getRebalanceProtocol returns the active rebalance protocol.
// Returns ProtocolEager if protocol cannot be determined (safe fallback).
func (a *Adapter) getRebalanceProtocol() RebalanceProtocol {
	if a.getRebalanceProtocolFn == nil {
		return ProtocolEager
	}
	return parseRebalanceProtocol(a.getRebalanceProtocolFn())
}
