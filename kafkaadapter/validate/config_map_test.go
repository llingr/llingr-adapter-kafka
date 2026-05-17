// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package validate

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// --- ConfigMap Tests ---

func TestConfigMap_ValidConfig(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "test-group",
	}

	// should not panic
	ConfigMap(configMap)

	// verify auto commit was disabled
	val, err := configMap.Get("enable.auto.commit", nil)
	if err != nil {
		t.Fatalf("failed to get enable.auto.commit: %v", err)
	}
	if val != false {
		t.Errorf("expected enable.auto.commit=false, got %v", val)
	}

	// verify auto offset store was disabled
	val, err = configMap.Get("enable.auto.offset.store", nil)
	if err != nil {
		t.Fatalf("failed to get enable.auto.offset.store: %v", err)
	}
	if val != false {
		t.Errorf("expected enable.auto.offset.store=false, got %v", val)
	}
}

// --- ensureAutoCommitDisabled Tests ---

func TestEnsureAutoCommitDisabled_NotSet(t *testing.T) {
	configMap := &kafka.ConfigMap{}

	ensureAutoCommitDisabled(configMap)

	val, _ := configMap.Get("enable.auto.commit", nil)
	if val != false {
		t.Errorf("expected false, got %v", val)
	}
}

func TestEnsureAutoCommitDisabled_SetFalse(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"enable.auto.commit": false,
	}

	// should not panic
	ensureAutoCommitDisabled(configMap)
}

func TestEnsureAutoCommitDisabled_SetTrue_Panics(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"enable.auto.commit": true,
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when auto commit is true")
		}
	}()

	ensureAutoCommitDisabled(configMap)
}

// --- ensureAutoOffsetStoreDisabled Tests ---

func TestEnsureAutoOffsetStoreDisabled_NotSet(t *testing.T) {
	configMap := &kafka.ConfigMap{}

	ensureAutoOffsetStoreDisabled(configMap)

	val, _ := configMap.Get("enable.auto.offset.store", nil)
	if val != false {
		t.Errorf("expected false, got %v", val)
	}
}

func TestEnsureAutoOffsetStoreDisabled_SetFalse(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"enable.auto.offset.store": false,
	}

	// should not panic
	ensureAutoOffsetStoreDisabled(configMap)
}

func TestEnsureAutoOffsetStoreDisabled_SetTrue_Panics(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"enable.auto.offset.store": true,
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when auto offset store is true")
		}
	}()

	ensureAutoOffsetStoreDisabled(configMap)
}

// --- logCooperativeStrategy Tests ---

func TestLogCooperativeStrategy_NotSet(_ *testing.T) {
	configMap := &kafka.ConfigMap{}

	// should not panic - logs nothing
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_Range(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "range",
	}

	// should not panic - logs nothing (not cooperative)
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_RoundRobin(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "roundrobin",
	}

	// should not panic - logs nothing (not cooperative)
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_Sticky(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "sticky",
	}

	// should not panic - logs nothing (not cooperative)
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_CooperativeSticky(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "cooperative-sticky",
	}

	// should not panic - just logs informational message
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_CooperativeRange(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "cooperative-range",
	}

	// should not panic - just logs informational message
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_CaseInsensitive(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "COOPERATIVE-STICKY",
	}

	// should not panic - case insensitive detection
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_ContainsCooperative(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "range,cooperative-sticky",
	}

	// should not panic - detects cooperative in mixed list
	logCooperativeStrategy(configMap)
}

func TestLogCooperativeStrategy_NotSet_NoLog(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	configMap := &kafka.ConfigMap{}
	logCooperativeStrategy(configMap)

	if buf.Len() > 0 {
		t.Errorf("expected no log output for unset key, got: %s", buf.String())
	}
}

func TestLogCooperativeStrategy_Cooperative_Logs(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": "cooperative-sticky",
	}
	logCooperativeStrategy(configMap)

	if !strings.Contains(buf.String(), "cooperative") {
		t.Errorf("expected log to contain 'cooperative', got: %s", buf.String())
	}
}

// --- rejectReadUncommitted Tests ---

func TestRejectReadUncommitted_NotSet(_ *testing.T) {
	configMap := &kafka.ConfigMap{}

	// should not panic (uses default read_committed)
	rejectReadUncommitted(configMap)
}

func TestRejectReadUncommitted_ReadCommitted(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"isolation.level": "read_committed",
	}

	// should not panic
	rejectReadUncommitted(configMap)
}

func TestRejectReadUncommitted_ReadUncommitted_Panics(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"isolation.level": "read_uncommitted",
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for read_uncommitted")
		}
	}()

	rejectReadUncommitted(configMap)
}

func TestRejectReadUncommitted_CaseInsensitive(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"isolation.level": "READ_UNCOMMITTED",
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for uppercase READ_UNCOMMITTED")
		}
	}()

	rejectReadUncommitted(configMap)
}

// --- rejectEventsChannelMode Tests ---

func TestRejectEventsChannelMode_NotSet(_ *testing.T) {
	configMap := &kafka.ConfigMap{}

	// should not panic
	rejectEventsChannelMode(configMap)
}

func TestRejectEventsChannelMode_False(_ *testing.T) {
	configMap := &kafka.ConfigMap{
		"go.events.channel.enable": false,
	}

	// should not panic
	rejectEventsChannelMode(configMap)
}

func TestRejectEventsChannelMode_True_Panics(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"go.events.channel.enable": true,
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when events channel mode is enabled")
		}
	}()

	rejectEventsChannelMode(configMap)
}

// --- warnIfSessionTimeoutTooLow Tests ---

func TestWarnIfSessionTimeoutTooLow_NotSet(t *testing.T) {
	configMap := &kafka.ConfigMap{}

	output := captureLogOutput(func() {
		warnIfSessionTimeoutTooLow(configMap)
	})

	if output != "" {
		t.Errorf("expected no warning for unset value, got: %s", output)
	}
}

func TestWarnIfSessionTimeoutTooLow_AboveThreshold(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"session.timeout.ms": 30000,
	}

	output := captureLogOutput(func() {
		warnIfSessionTimeoutTooLow(configMap)
	})

	if output != "" {
		t.Errorf("expected no warning for value above threshold, got: %s", output)
	}
}

func TestWarnIfSessionTimeoutTooLow_AtThreshold(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"session.timeout.ms": 25000,
	}

	output := captureLogOutput(func() {
		warnIfSessionTimeoutTooLow(configMap)
	})

	if output != "" {
		t.Errorf("expected no warning for value at threshold, got: %s", output)
	}
}

func TestWarnIfSessionTimeoutTooLow_BelowThreshold(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"session.timeout.ms": 10000,
	}

	output := captureLogOutput(func() {
		warnIfSessionTimeoutTooLow(configMap)
	})

	if !strings.Contains(output, "session.timeout.ms") {
		t.Errorf("expected warning about session.timeout.ms, got: %s", output)
	}
	if !strings.Contains(output, "10000") {
		t.Errorf("expected warning to include actual value 10000, got: %s", output)
	}
}

// --- warnIfMaxPollIntervalTooLow Tests ---

func TestWarnIfMaxPollIntervalTooLow_NotSet(t *testing.T) {
	configMap := &kafka.ConfigMap{}

	output := captureLogOutput(func() {
		warnIfMaxPollIntervalTooLow(configMap)
	})

	if output != "" {
		t.Errorf("expected no warning for unset value, got: %s", output)
	}
}

func TestWarnIfMaxPollIntervalTooLow_AboveThreshold(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"max.poll.interval.ms": 60000,
	}

	output := captureLogOutput(func() {
		warnIfMaxPollIntervalTooLow(configMap)
	})

	if output != "" {
		t.Errorf("expected no warning for value above threshold, got: %s", output)
	}
}

func TestWarnIfMaxPollIntervalTooLow_AtThreshold(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"max.poll.interval.ms": 30000,
	}

	output := captureLogOutput(func() {
		warnIfMaxPollIntervalTooLow(configMap)
	})

	if output != "" {
		t.Errorf("expected no warning for value at threshold, got: %s", output)
	}
}

func TestWarnIfMaxPollIntervalTooLow_BelowThreshold(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"max.poll.interval.ms": 20000,
	}

	output := captureLogOutput(func() {
		warnIfMaxPollIntervalTooLow(configMap)
	})

	if !strings.Contains(output, "max.poll.interval.ms") {
		t.Errorf("expected warning about max.poll.interval.ms, got: %s", output)
	}
	if !strings.Contains(output, "20000") {
		t.Errorf("expected warning to include actual value 20000, got: %s", output)
	}
}

// --- Edge Cases ---

func TestConfigValidation_NonBoolValue(_ *testing.T) {
	// test with non-bool value for bool config
	configMap := &kafka.ConfigMap{
		"enable.auto.commit": "false", // string instead of bool
	}

	// should not panic - string "false" is not bool true
	ensureAutoCommitDisabled(configMap)
}

func TestConfigValidation_NonBoolAutoOffsetStore(_ *testing.T) {
	// test with non-bool value for auto.offset.store config
	configMap := &kafka.ConfigMap{
		"enable.auto.offset.store": "false", // string instead of bool
	}

	// should not panic - string "false" is not bool true
	ensureAutoOffsetStoreDisabled(configMap)
}

func TestConfigValidation_NonStringStrategy(_ *testing.T) {
	// test with non-string value for strategy
	configMap := &kafka.ConfigMap{
		"partition.assignment.strategy": 123, // int instead of string
	}

	// should not panic - wrong type is ignored
	logCooperativeStrategy(configMap)
}

func TestConfigValidation_NonStringIsolationLevel(_ *testing.T) {
	// test with non-string value for isolation.level
	configMap := &kafka.ConfigMap{
		"isolation.level": 123, // int instead of string
	}

	// should not panic - wrong type is ignored
	rejectReadUncommitted(configMap)
}

func TestConfigValidation_NonBoolEventsChannel(_ *testing.T) {
	// test with non-bool value for events channel
	configMap := &kafka.ConfigMap{
		"go.events.channel.enable": "true", // string instead of bool
	}

	// should not panic - wrong type is ignored
	rejectEventsChannelMode(configMap)
}

func TestConfigValidation_NonIntTimeout(t *testing.T) {
	// test with non-int value for timeout
	configMap := &kafka.ConfigMap{
		"session.timeout.ms": "10000", // string instead of int
	}

	output := captureLogOutput(func() {
		warnIfSessionTimeoutTooLow(configMap)
	})

	// should not warn - wrong type is ignored
	if output != "" {
		t.Errorf("expected no warning for wrong type, got: %s", output)
	}
}

func TestConfigValidation_NonIntMaxPollInterval(t *testing.T) {
	// test with non-int value for max.poll.interval.ms
	configMap := &kafka.ConfigMap{
		"max.poll.interval.ms": "20000", // string instead of int
	}

	output := captureLogOutput(func() {
		warnIfMaxPollIntervalTooLow(configMap)
	})

	// should not warn - wrong type is ignored
	if output != "" {
		t.Errorf("expected no warning for wrong type, got: %s", output)
	}
}

// --- Full Integration Test ---

func TestConfigMap_AllValidationsRun(t *testing.T) {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":             "localhost:9092",
		"group.id":                      "test-group",
		"partition.assignment.strategy": "range",
		"isolation.level":               "read_committed",
		"session.timeout.ms":            45000,
		"max.poll.interval.ms":          300000,
	}

	// should not panic
	ConfigMap(configMap)

	// verify auto settings were applied
	autoCommit, _ := configMap.Get("enable.auto.commit", nil)
	if autoCommit != false {
		t.Error("expected enable.auto.commit to be false")
	}

	autoStore, _ := configMap.Get("enable.auto.offset.store", nil)
	if autoStore != false {
		t.Error("expected enable.auto.offset.store to be false")
	}
}

// --- Helper Functions ---

func captureLogOutput(f func()) string {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	f()

	return buf.String()
}
