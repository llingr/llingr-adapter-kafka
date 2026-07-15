// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/llingr/llingr-nexus/nexus"
)

// closeOnPanic must call closeFn and swallow any secondary panic it raises, so
// closing a consumer while a primary panic propagates through CreateConsumer can
// never mask that primary panic.
func TestCloseOnPanicSwallowsSecondaryPanic(t *testing.T) {
	closed := false
	a := &Adapter{closeFn: func() error { closed = true; panic("close boom") }}
	a.closeOnPanic() // must return normally despite closeFn panicking
	if !closed {
		t.Fatal("closeOnPanic did not call closeFn")
	}
}

// A nil closeFn (e.g. a NewCustom adapter before SetConsumer) must be a safe no-op.
func TestCloseOnPanicNilCloseFn(t *testing.T) {
	a := &Adapter{}
	a.closeOnPanic() // must not panic
}

// panicBuilder is a minimal nexus.ConsumerBuilder whose Build panics, standing
// in for the engine's builder.Build panicking on an out-of-range DemuxConfig.
type panicBuilder struct{ topic string }

func (b panicBuilder) TopicName() string { return b.topic }
func (b panicBuilder) Build(nexus.BrokerPort[*kafka.Message]) nexus.AdaptedConsumer[*kafka.Message] {
	panic("build boom")
}

// When builder.Build panics and the adapter OWNS the consumer (the New path),
// CreateConsumer must close it exactly once and re-propagate the original panic.
func TestCreateConsumerClosesOwnedConsumerOnBuildPanic(t *testing.T) {
	closed := 0
	a := &Adapter{ownsConsumer: true, closeFn: func() error { closed++; return nil }}
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected the build panic to propagate")
		}
		if r != "build boom" {
			t.Fatalf("panic value = %v, want \"build boom\"", r)
		}
		if closed != 1 {
			t.Fatalf("closeFn called %d times, want exactly 1", closed)
		}
	}()
	a.CreateConsumer(panicBuilder{topic: "t"})
	t.Fatal("CreateConsumer should have panicked")
}

// A consumer supplied via NewCustom+SetConsumer is owned by the caller;
// CreateConsumer must re-propagate the panic but NOT close it.
func TestCreateConsumerLeavesUserConsumerOnBuildPanic(t *testing.T) {
	closed := 0
	a := &Adapter{ownsConsumer: false, closeFn: func() error { closed++; return nil }}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected the build panic to propagate")
		}
		if closed != 0 {
			t.Fatalf("user-supplied consumer must not be closed, got %d close(s)", closed)
		}
	}()
	a.CreateConsumer(panicBuilder{topic: "t"})
}
