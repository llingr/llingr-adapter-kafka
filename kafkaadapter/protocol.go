// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

// RebalanceProtocol indicates the partition assignment protocol negotiated with the broker.
//
// The protocol determines which librdkafka APIs are used for partition management:
//   - Eager: Assign()/Unassign() - stop-the-world, all partitions revoked before reassignment
//   - Cooperative: IncrementalAssign()/IncrementalUnassign() - only changing partitions affected
//
// The protocol is discovered at runtime via GetRebalanceProtocol() after the consumer joins
// the group. The broker selects the protocol based on the intersection of all group members'
// supported strategies (partition.assignment.strategy config).
type RebalanceProtocol int

const (
	// ProtocolEager uses stop-the-world rebalancing where all partitions are revoked
	// before reassignment. This is the traditional Kafka behaviour and the default
	// when using strategies like "range" or "roundrobin".
	//
	// APIs used: Assign(), Unassign()
	ProtocolEager RebalanceProtocol = iota + 1

	// ProtocolCooperative uses incremental rebalancing where only partitions that are
	// changing ownership are revoked/assigned. Consumers continue processing unchanged
	// partitions during rebalance. Requires all group members to support cooperative
	// strategies (e.g., "cooperative-sticky").
	//
	// APIs used: IncrementalAssign(), IncrementalUnassign()
	ProtocolCooperative
)

// parseRebalanceProtocol converts librdkafka's protocol string to RebalanceProtocol.
//
// librdkafka's GetRebalanceProtocol() returns:
//   - "EAGER" for eager/stop-the-world protocols
//   - "COOPERATIVE" for incremental protocols
//   - "" if not yet known (before joining group)
//
// Returns ProtocolEager for unknown/empty values as safe fallback - using Assign()/Unassign()
// on a cooperative consumer works (sub-optimally), but using IncrementalAssign()/
// IncrementalUnassign() on an eager consumer breaks partition tracking.
func parseRebalanceProtocol(protocol string) RebalanceProtocol {
	if protocol == "COOPERATIVE" {
		return ProtocolCooperative
	}
	return ProtocolEager
}
