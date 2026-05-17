// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/llingr/llingr-nexus/nexus"
)

// bandwidthCollector parses librdkafka stats JSON and invokes the
// registered callback with a BandwidthMetrics packet. The timer is
// owned by librdkafka (statistics.interval.ms), not by this code.
//
// librdkafka emits cumulative counters (from client start) on each
// statistics interval. This collector tracks the previous snapshot
// and emits deltas, ensuring downstream consumers (prometheus sink,
// aggregator) receive per-interval increments rather than ever-growing
// totals. This is correct for prometheus counters that use Add().
type bandwidthCollector struct {
	callback  nexus.BandwidthCallback
	interval  time.Duration
	groupID   string
	topicName string

	// prevPartitions stores the most recent cumulative counters from
	// librdkafka, keyed by partition ID. On each stats event, the
	// delta (current - previous) is emitted and prevPartitions is
	// updated to the current snapshot.
	prevPartitions map[int32]partitionSnapshot
}

// partitionSnapshot stores the cumulative counters from one librdkafka
// stats event for a single partition.
type partitionSnapshot struct {
	rxBytes int64
	txBytes int64
	rxMsgs  int64
}

// librdkafkaStats is a minimal parse target for the librdkafka JSON stats blob.
// Only the fields needed for bandwidth metrics are included.
type librdkafkaStats struct {
	Brokers map[string]librdkafkaBroker `json:"brokers"`
	Topics  map[string]librdkafkaTopic  `json:"topics"`
}

type librdkafkaBroker struct {
	NodeID  int    `json:"nodeid"`
	Name    string `json:"name"`
	Host    string `json:"host"`
	Port    int    `json:"port"`
	Rack    string `json:"rack"`
	TxBytes int64  `json:"txbytes"`
	RxBytes int64  `json:"rxbytes"`
}

type librdkafkaTopic struct {
	Partitions map[string]librdkafkaPartition `json:"partitions"`
}

type librdkafkaPartition struct {
	Partition int32 `json:"partition"`
	Leader    int   `json:"leader"`
	RxBytes   int64 `json:"rxbytes"`
	TxBytes   int64 `json:"txbytes"`
	RxMsgs    int64 `json:"rxmsgs"`
}

// handleStats parses a librdkafka stats JSON blob and invokes the callback
// with a delta-based BandwidthMetrics packet. librdkafka counters are
// cumulative from client start; this method subtracts the previous snapshot
// to produce per-interval deltas.
func (bc *bandwidthCollector) handleStats(jsonStr string) {
	if bc.callback == nil {
		return
	}

	var stats librdkafkaStats
	if err := json.Unmarshal([]byte(jsonStr), &stats); err != nil {
		return // silently skip malformed stats
	}

	now := time.Now()

	// extract broker metadata
	brokers := make([]nexus.BrokerInfo, 0, len(stats.Brokers))
	for _, b := range stats.Brokers {
		// skip internal/bootstrap brokers (negative node IDs)
		if b.NodeID < 0 {
			continue
		}
		brokers = append(brokers, nexus.BrokerInfo{
			ID:   strconv.Itoa(b.NodeID),
			Host: b.Host,
			Port: strconv.Itoa(b.Port),
			Rack: b.Rack,
		})
	}

	// extract partition bandwidth for the subscribed topic, computing
	// deltas against the previous snapshot
	var partitions []nexus.PartitionBandwidth
	if topicStats, ok := stats.Topics[bc.topicName]; ok {
		if bc.prevPartitions == nil {
			bc.prevPartitions = make(map[int32]partitionSnapshot, len(topicStats.Partitions))
		}

		partitions = make([]nexus.PartitionBandwidth, 0, len(topicStats.Partitions))
		for _, p := range topicStats.Partitions {
			// skip aggregate partition (-1)
			if p.Partition < 0 {
				continue
			}

			prev := bc.prevPartitions[p.Partition]

			// delta = current cumulative - previous cumulative
			// Guard against counter resets (e.g. broker restart) by
			// clamping negative deltas to zero.
			deltaRx := p.RxBytes - prev.rxBytes
			if deltaRx < 0 {
				deltaRx = 0
			}
			deltaTx := p.TxBytes - prev.txBytes
			if deltaTx < 0 {
				deltaTx = 0
			}
			deltaMsgs := p.RxMsgs - prev.rxMsgs
			if deltaMsgs < 0 {
				deltaMsgs = 0
			}

			partitions = append(partitions, nexus.PartitionBandwidth{
				Ts:                   now,
				ID:                   p.Partition,
				Leader:               strconv.Itoa(p.Leader),
				ReceivedBytes:        deltaRx,
				TransmittedBytes:     deltaTx,
				ReceivedMessageCount: deltaMsgs,
				// Compression fields: zero (librdkafka stats don't expose per-partition compression)
			})

			// update snapshot to current cumulative values
			bc.prevPartitions[p.Partition] = partitionSnapshot{
				rxBytes: p.RxBytes,
				txBytes: p.TxBytes,
				rxMsgs:  p.RxMsgs,
			}
		}
	}

	packet := nexus.BandwidthMetrics{
		Ts:                    now,
		StatsIntervalDuration: bc.interval,
		BandwidthMetricsID:    generateUUID(),
		TopicName:             bc.topicName,
		ConsumerGroup:         bc.groupID,
		Brokers:               brokers,
		Partitions:            partitions,
	}

	bc.callback(packet)
}

// generateUUID returns a v4 UUID string using crypto/rand.
func generateUUID() string {
	var uuid [16]byte
	_, _ = rand.Read(uuid[:])
	uuid[6] = (uuid[6] & 0x0f) | 0x40 // version 4
	uuid[8] = (uuid[8] & 0x3f) | 0x80 // variant 2
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])
}

// --- BandwidthPort[*kafka.Message] implementation on Adapter ---

// SetBandwidthCallback registers the framework callback for bandwidth telemetry.
// Called by the host's nexus.ConsumerBuilder during Build() as part of the
// BandwidthPort wiring; users do not call this directly.
func (a *Adapter) SetBandwidthCallback(cb nexus.BandwidthCallback) {
	if a.bwCollector == nil {
		return
	}
	a.bwCollector.callback = cb
}

// StatsInterval returns the configured bandwidth collection cadence.
func (a *Adapter) StatsInterval() time.Duration {
	if a.bwCollector == nil {
		return nexus.DefaultBandwidthInterval
	}
	return a.bwCollector.interval
}
