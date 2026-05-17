// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import "testing"

func TestParseRebalanceProtocol_Cooperative(t *testing.T) {
	result := parseRebalanceProtocol("COOPERATIVE")

	if result != ProtocolCooperative {
		t.Errorf("expected ProtocolCooperative, got %v", result)
	}
}

func TestParseRebalanceProtocol_Eager(t *testing.T) {
	result := parseRebalanceProtocol("EAGER")

	if result != ProtocolEager {
		t.Errorf("expected ProtocolEager, got %v", result)
	}
}

func TestParseRebalanceProtocol_EmptyString(t *testing.T) {
	// empty string means protocol not yet known - should default to eager
	result := parseRebalanceProtocol("")

	if result != ProtocolEager {
		t.Errorf("expected ProtocolEager for empty string, got %v", result)
	}
}

func TestParseRebalanceProtocol_UnknownValue(t *testing.T) {
	// unknown values should default to eager (safe fallback)
	result := parseRebalanceProtocol("UNKNOWN")

	if result != ProtocolEager {
		t.Errorf("expected ProtocolEager for unknown value, got %v", result)
	}
}

func TestParseRebalanceProtocol_CaseSensitive(t *testing.T) {
	// librdkafka returns uppercase, but test lowercase is handled safely
	tests := []struct {
		input    string
		expected RebalanceProtocol
	}{
		{"cooperative", ProtocolEager}, // lowercase not matched, falls back to eager
		{"Cooperative", ProtocolEager}, // mixed case not matched
		{"COOPERATIVE", ProtocolCooperative},
	}

	for _, tt := range tests {
		result := parseRebalanceProtocol(tt.input)
		if result != tt.expected {
			t.Errorf("parseRebalanceProtocol(%q) = %v, expected %v", tt.input, result, tt.expected)
		}
	}
}

func TestRebalanceProtocol_DistinctValues(t *testing.T) {
	// ensure protocols have distinct non-zero values
	if ProtocolEager == 0 {
		t.Error("ProtocolEager should not be zero")
	}
	if ProtocolCooperative == 0 {
		t.Error("ProtocolCooperative should not be zero")
	}
	if ProtocolEager == ProtocolCooperative {
		t.Error("protocols should have distinct values")
	}
}
