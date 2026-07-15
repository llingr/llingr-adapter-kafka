// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"sync"
	"sync/atomic"
	"testing"
)

// newReleaseAdapter wires an adapter whose unsubscribeFn and closeFn are
// replaced with counters, so tests can assert how often the release ran.
func newReleaseAdapter(owns bool) (*Adapter, *atomic.Int32, *atomic.Int32) {
	adapter := NewCustom()
	adapter.ownsConsumer = owns

	leaves := &atomic.Int32{}
	closes := &atomic.Int32{}
	adapter.unsubscribeFn = func() error { leaves.Add(1); return nil }
	adapter.closeFn = func() error { closes.Add(1); return nil }
	return adapter, leaves, closes
}

// An owned consumer (the New path) is both left and closed by Unsubscribe.
func TestUnsubscribe_OwnedConsumerIsClosed(t *testing.T) {
	adapter, leaves, closes := newReleaseAdapter(true)

	if err := adapter.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if got := leaves.Load(); got != 1 {
		t.Errorf("group leave ran %d times, want 1", got)
	}
	if got := closes.Load(); got != 1 {
		t.Errorf("close ran %d times, want 1", got)
	}
}

// A consumer supplied via SetConsumer is left but NOT closed: it belongs to
// its owner, who may keep using it after the group is left.
func TestUnsubscribe_CustomConsumerIsNotClosed(t *testing.T) {
	adapter, leaves, closes := newReleaseAdapter(false)

	if err := adapter.Unsubscribe(); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	if got := leaves.Load(); got != 1 {
		t.Errorf("group leave ran %d times, want 1", got)
	}
	if got := closes.Load(); got != 0 {
		t.Errorf("close ran %d times on a user-owned consumer, want 0", got)
	}
}

// Unsubscribe is idempotent: sequential and concurrent callers (the engine's
// drain racing a host-side emergency cleanup) produce exactly one leave and
// one close, and every caller returns only once the release completed.
func TestUnsubscribe_IdempotentAcrossCallers(t *testing.T) {
	adapter, leaves, closes := newReleaseAdapter(true)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = adapter.Unsubscribe()
		}()
	}
	wg.Wait()
	_ = adapter.Unsubscribe() // late sequential repeat

	if got := leaves.Load(); got != 1 {
		t.Errorf("group leave ran %d times, want exactly 1", got)
	}
	if got := closes.Load(); got != 1 {
		t.Errorf("close ran %d times, want exactly 1", got)
	}
}
