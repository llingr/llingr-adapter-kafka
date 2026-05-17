// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

// recordingLogger captures Warn calls so the diagnostic hint can be asserted.
// Other levels are kept (and discarded) so the surface matches mockLogger.
type recordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (r *recordingLogger) Debug(_ context.Context, _ string, _ ...any) {}
func (r *recordingLogger) Info(_ context.Context, _ string, _ ...any)  {}
func (r *recordingLogger) Error(_ context.Context, _ string, _ ...any) {}
func (r *recordingLogger) Warn(_ context.Context, format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warns = append(r.warns, fmt.Sprintf(format, args...))
}

func (r *recordingLogger) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.warns))
	copy(out, r.warns)
	return out
}

// --- isGroupMembershipLost ---

func TestIsGroupMembershipLost_TypedUnknownMember(t *testing.T) {
	err := kafka.NewError(kafka.ErrUnknownMemberID, "Unknown member", false)
	if !isGroupMembershipLost(err) {
		t.Fatalf("expected typed ErrUnknownMemberID to be classified as group-membership-lost")
	}
}

func TestIsGroupMembershipLost_TypedIllegalGeneration(t *testing.T) {
	err := kafka.NewError(kafka.ErrIllegalGeneration, "Illegal generation", false)
	if !isGroupMembershipLost(err) {
		t.Fatalf("expected typed ErrIllegalGeneration to be classified as group-membership-lost")
	}
}

func TestIsGroupMembershipLost_TypedUnrelatedError(t *testing.T) {
	err := kafka.NewError(kafka.ErrTimedOut, "Timed out", false)
	if isGroupMembershipLost(err) {
		t.Fatalf("ErrTimedOut should not be classified as group-membership-lost")
	}
}

func TestIsGroupMembershipLost_TypedRebalanceInProgressNotMatched(t *testing.T) {
	// RebalanceInProgress is adjacent semantics but not the kicked-out signal -
	// keep the false-positive surface tight.
	err := kafka.NewError(kafka.ErrRebalanceInProgress, "Rebalance in progress", false)
	if isGroupMembershipLost(err) {
		t.Fatalf("ErrRebalanceInProgress should not be classified as group-membership-lost (adjacent but distinct)")
	}
}

func TestIsGroupMembershipLost_StringFallback_UnknownMember(t *testing.T) {
	// Simulate an FFI-edge case where the typed error didn't surface but the
	// raw string does identify the failure mode.
	err := errors.New("rdkafka commit error: Broker: Unknown member")
	if !isGroupMembershipLost(err) {
		t.Fatalf("expected string-fallback to catch 'Unknown member' substring")
	}
}

func TestIsGroupMembershipLost_StringFallback_IllegalGeneration(t *testing.T) {
	err := errors.New("commit failed: Broker: Illegal generation")
	if !isGroupMembershipLost(err) {
		t.Fatalf("expected string-fallback to catch 'Illegal generation' substring")
	}
}

func TestIsGroupMembershipLost_StringFallback_CaseInsensitive(t *testing.T) {
	err := errors.New("UNKNOWN MEMBER ID")
	if !isGroupMembershipLost(err) {
		t.Fatalf("expected case-insensitive matching")
	}
}

func TestIsGroupMembershipLost_StringFallback_UnrelatedMessage(t *testing.T) {
	err := errors.New("network timeout while committing offsets")
	if isGroupMembershipLost(err) {
		t.Fatalf("unrelated error should not match")
	}
}

func TestIsGroupMembershipLost_NilSafety(t *testing.T) {
	// Defensive: helper should never panic. nil is an unusual input but the
	// caller's contract is that we receive an error - so if we ever get nil,
	// not-applicable is the safest answer.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("isGroupMembershipLost panicked on nil: %v", r)
		}
	}()
	// Wrap nil to avoid passing nil directly (which is a separate language quirk).
	err := errors.Join(nil, nil)
	_ = isGroupMembershipLost(err)
}

// --- currentSessionTimeoutDescription ---

func TestCurrentSessionTimeoutDescription_NilConfigMap(t *testing.T) {
	// NewCustom does not set configMap; the diagnostic should clearly say so.
	a := NewCustom()
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "NewCustom") {
		t.Fatalf("expected description to indicate NewCustom origin, got %q", got)
	}
}

func TestCurrentSessionTimeoutDescription_ConfigMapWithoutSessionTimeout(t *testing.T) {
	// configMap present but session.timeout.ms not set -> librdkafka default applies.
	cm := &kafka.ConfigMap{"bootstrap.servers": "localhost:9092"}
	a := &Adapter{configMap: cm}
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "default") {
		t.Fatalf("expected description to flag librdkafka default, got %q", got)
	}
}

func TestCurrentSessionTimeoutDescription_ConfigMapWithExplicitValue(t *testing.T) {
	cm := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"session.timeout.ms": 6000,
	}
	a := &Adapter{configMap: cm}
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "6000") {
		t.Fatalf("expected description to include explicit value 6000, got %q", got)
	}
}

func TestCurrentSessionTimeoutDescription_ConfigMapWithStringValue(t *testing.T) {
	// confluent-kafka-go accepts both numeric and string values in the ConfigMap.
	// The description should accept either via fmt %v.
	cm := &kafka.ConfigMap{
		"bootstrap.servers":  "localhost:9092",
		"session.timeout.ms": "10000",
	}
	a := &Adapter{configMap: cm}
	got := a.currentSessionTimeoutDescription()
	if !strings.Contains(got, "10000") {
		t.Fatalf("expected description to include string-form value 10000, got %q", got)
	}
}

// --- logGroupMembershipHintIfApplicable ---

func TestLogGroupMembershipHint_FiresOnUnknownMember(t *testing.T) {
	rec := &recordingLogger{}
	a := &Adapter{
		ctx:    context.Background(),
		logger: rec,
		configMap: &kafka.ConfigMap{
			"session.timeout.ms": 6000,
		},
	}

	a.logGroupMembershipHintIfApplicable(kafka.NewError(kafka.ErrUnknownMemberID, "Unknown member", false))

	warns := rec.snapshot()
	if len(warns) != 1 {
		t.Fatalf("expected one Warn log, got %d: %v", len(warns), warns)
	}
	if !strings.Contains(warns[0], "group membership lost") {
		t.Errorf("warn missing 'group membership lost' phrase: %q", warns[0])
	}
	if !strings.Contains(warns[0], "6000") {
		t.Errorf("warn should include configured session.timeout.ms value: %q", warns[0])
	}
	if !strings.Contains(warns[0], "session.timeout.ms") {
		t.Errorf("warn should reference the config knob to raise: %q", warns[0])
	}
}

func TestLogGroupMembershipHint_FiresOnIllegalGeneration(t *testing.T) {
	rec := &recordingLogger{}
	a := &Adapter{ctx: context.Background(), logger: rec}

	a.logGroupMembershipHintIfApplicable(kafka.NewError(kafka.ErrIllegalGeneration, "Illegal generation", false))

	if got := len(rec.snapshot()); got != 1 {
		t.Fatalf("expected one Warn log, got %d", got)
	}
}

func TestLogGroupMembershipHint_SilentOnUnrelatedError(t *testing.T) {
	rec := &recordingLogger{}
	a := &Adapter{ctx: context.Background(), logger: rec}

	a.logGroupMembershipHintIfApplicable(errors.New("network unreachable"))
	a.logGroupMembershipHintIfApplicable(kafka.NewError(kafka.ErrTimedOut, "timed out", false))

	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("expected zero Warn logs for unrelated errors, got %d: %v", got, rec.snapshot())
	}
}

func TestLogGroupMembershipHint_FiresOnStringFallback(t *testing.T) {
	rec := &recordingLogger{}
	a := &Adapter{ctx: context.Background(), logger: rec}

	a.logGroupMembershipHintIfApplicable(errors.New("rdkafka raw: Broker: Unknown member id"))

	if got := len(rec.snapshot()); got != 1 {
		t.Fatalf("expected string-fallback to trigger Warn, got %d logs", got)
	}
}

// --- CommitOffsets integration: hint surfaces alongside the Error log ---

func TestCommitOffsets_UnknownMember_LogsDiagnosticHint(t *testing.T) {
	rec := &recordingLogger{}
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = rec
	adapter.configMap = &kafka.ConfigMap{
		"session.timeout.ms": 6000,
	}

	expectedErr := kafka.NewError(kafka.ErrUnknownMemberID, "Unknown member", false)
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
	msg := &nexus.Message[*kafka.Message]{Payload: &kafkaMsg}

	_, err := adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected wrapped error to match, got %v", err)
	}

	warns := rec.snapshot()
	if len(warns) != 1 {
		t.Fatalf("expected exactly one diagnostic Warn, got %d: %v", len(warns), warns)
	}
	if !strings.Contains(warns[0], "group membership lost") {
		t.Errorf("Warn should describe the failure mode: %q", warns[0])
	}
	if !strings.Contains(warns[0], "6000") {
		t.Errorf("Warn should surface the configured session.timeout.ms value: %q", warns[0])
	}
}

func TestCommitOffsets_GenericFailure_NoHint(t *testing.T) {
	// Non-membership commit failures should still log the Error but must not
	// trigger the misleading "raise session.timeout.ms" hint.
	rec := &recordingLogger{}
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = rec
	adapter.commitOffsetsFn = func(_ []kafka.TopicPartition) ([]kafka.TopicPartition, error) {
		return nil, errors.New("generic commit failure")
	}

	topic := testTopicName
	kafkaMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &topic, Partition: 0, Offset: 0,
		},
	}
	msg := &nexus.Message[*kafka.Message]{Payload: &kafkaMsg}

	_, _ = adapter.CommitOffsets([]*nexus.Message[*kafka.Message]{msg})

	if got := len(rec.snapshot()); got != 0 {
		t.Fatalf("generic commit failure must not produce diagnostic Warn, got %d: %v", got, rec.snapshot())
	}
}
