// SPDX-FileCopyrightText: Copyright (c) 2025 The llingr-adapter-kafka Authors
// SPDX-License-Identifier: Apache-2.0

package kafkaadapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// --- handleOAuthTokenRefresh Tests ---

func TestHandleOAuthTokenRefresh_NoHandler(t *testing.T) {
	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.kafkaConsumer = nil // can't set mock directly due to type

	// verify the handler is nil initially
	if adapter.oauthTokenRefreshFn != nil {
		t.Error("expected no OAuth handler initially")
	}

	// after setting one, it should work
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return kafka.OAuthBearerToken{TokenValue: "test"}, nil
	})

	if adapter.oauthTokenRefreshFn == nil {
		t.Error("expected OAuth handler to be set")
	}
}

func TestHandleOAuthTokenRefresh_HandlerSuccess(t *testing.T) {
	expectedToken := kafka.OAuthBearerToken{
		TokenValue: "access-token-123",
		Expiration: time.Now().Add(1 * time.Hour),
		Principal:  "user@example.com",
	}

	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	var receivedToken kafka.OAuthBearerToken
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return expectedToken, nil
	})

	// call the handler and verify it returns expected token
	token, err := adapter.oauthTokenRefreshFn()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	receivedToken = token

	if receivedToken.TokenValue != expectedToken.TokenValue {
		t.Errorf("expected token value '%s', got '%s'",
			expectedToken.TokenValue, receivedToken.TokenValue)
	}
	if receivedToken.Principal != expectedToken.Principal {
		t.Errorf("expected principal '%s', got '%s'",
			expectedToken.Principal, receivedToken.Principal)
	}
}

func TestHandleOAuthTokenRefresh_HandlerError(t *testing.T) {
	expectedErr := errors.New("failed to fetch token")

	adapter := NewCustom()
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return kafka.OAuthBearerToken{}, expectedErr
	})

	token, err := adapter.oauthTokenRefreshFn()

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if token.TokenValue != "" {
		t.Errorf("expected empty token, got %s", token.TokenValue)
	}
}

func TestHandleOAuthTokenRefresh_MultipleRefreshes(t *testing.T) {
	callCount := 0
	tokens := []string{"token-1", "token-2", "token-3"}

	adapter := NewCustom()
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		token := tokens[callCount]
		callCount++
		return kafka.OAuthBearerToken{TokenValue: token}, nil
	})

	for i := 0; i < 3; i++ {
		token, err := adapter.oauthTokenRefreshFn()
		if err != nil {
			t.Fatalf("unexpected error on call %d: %v", i, err)
		}
		if token.TokenValue != tokens[i] {
			t.Errorf("call %d: expected token '%s', got '%s'", i, tokens[i], token.TokenValue)
		}
	}

	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestHandleOAuthTokenRefresh_OverwriteHandler(t *testing.T) {
	adapter := NewCustom()

	// set first handler
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return kafka.OAuthBearerToken{TokenValue: "first"}, nil
	})

	// overwrite with second handler
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return kafka.OAuthBearerToken{TokenValue: "second"}, nil
	})

	token, _ := adapter.oauthTokenRefreshFn()
	if token.TokenValue != "second" {
		t.Errorf("expected 'second', got '%s'", token.TokenValue)
	}
}

// --- Poll with OAuthBearerTokenRefresh Event ---

func TestPoll_OAuthBearerTokenRefresh_NoHandler(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}
	adapter.oauthTokenRefreshFn = nil // no handler

	failureCalled := false
	var failureReason string
	adapter.setOAuthBearerTokenFailureFn = func(reason string) error {
		failureCalled = true
		failureReason = reason
		return nil
	}

	event := kafka.OAuthBearerTokenRefresh{}
	adapter.pollFn = func(_ int) kafka.Event { return event }

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if msg != nil {
		t.Error("expected nil message")
	}
	if ok {
		t.Error("expected ok=false")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !failureCalled {
		t.Error("expected SetOAuthBearerTokenFailure to be called")
	}
	if failureReason != "no token refresh handler registered" {
		t.Errorf("unexpected failure reason: %s", failureReason)
	}
}

func TestPoll_OAuthBearerTokenRefresh_HandlerReturnsError(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	expectedErr := errors.New("token fetch failed")
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return kafka.OAuthBearerToken{}, expectedErr
	})

	failureCalled := false
	var failureReason string
	adapter.setOAuthBearerTokenFailureFn = func(reason string) error {
		failureCalled = true
		failureReason = reason
		return nil
	}

	event := kafka.OAuthBearerTokenRefresh{}
	adapter.pollFn = func(_ int) kafka.Event { return event }

	_, _, _ = adapter.Poll(100 * time.Millisecond)

	if !failureCalled {
		t.Error("expected SetOAuthBearerTokenFailure to be called")
	}
	if failureReason != "token fetch failed" {
		t.Errorf("expected failure reason 'token fetch failed', got '%s'", failureReason)
	}
}

func TestPoll_OAuthBearerTokenRefresh_SetTokenError(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return kafka.OAuthBearerToken{TokenValue: "valid-token"}, nil
	})

	setTokenErr := errors.New("failed to set token")
	adapter.setOAuthBearerTokenFn = func(_ kafka.OAuthBearerToken) error {
		return setTokenErr
	}

	failureCalled := false
	var failureReason string
	adapter.setOAuthBearerTokenFailureFn = func(reason string) error {
		failureCalled = true
		failureReason = reason
		return nil
	}

	event := kafka.OAuthBearerTokenRefresh{}
	adapter.pollFn = func(_ int) kafka.Event { return event }

	_, _, _ = adapter.Poll(100 * time.Millisecond)

	if !failureCalled {
		t.Error("expected SetOAuthBearerTokenFailure to be called on SetOAuthBearerToken error")
	}
	if failureReason != "failed to set token" {
		t.Errorf("expected failure reason 'failed to set token', got '%s'", failureReason)
	}
}

func TestPoll_OAuthBearerTokenRefresh_Success(t *testing.T) {
	adapter := NewCustom()
	adapter.isClosedFn = func() bool { return false }
	adapter.ctx = context.Background()
	adapter.logger = &mockLogger{}

	handlerCalled := false
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		handlerCalled = true
		return kafka.OAuthBearerToken{TokenValue: "valid-token"}, nil
	})

	var receivedToken kafka.OAuthBearerToken
	adapter.setOAuthBearerTokenFn = func(token kafka.OAuthBearerToken) error {
		receivedToken = token
		return nil
	}

	failureCalled := false
	adapter.setOAuthBearerTokenFailureFn = func(_ string) error {
		failureCalled = true
		return nil
	}

	event := kafka.OAuthBearerTokenRefresh{}
	adapter.pollFn = func(_ int) kafka.Event { return event }

	msg, ok, err := adapter.Poll(100 * time.Millisecond)

	if msg != nil {
		t.Error("expected nil message")
	}
	if ok {
		t.Error("expected ok=false")
	}
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("expected OAuth handler to be called")
	}
	if receivedToken.TokenValue != "valid-token" {
		t.Errorf("expected token 'valid-token', got '%s'", receivedToken.TokenValue)
	}
	if failureCalled {
		t.Error("SetOAuthBearerTokenFailure should not be called on success")
	}
}

// --- Token Validation ---

func TestOAuthToken_CompleteToken(t *testing.T) {
	now := time.Now()
	expiry := now.Add(1 * time.Hour)

	adapter := NewCustom()
	adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
		return kafka.OAuthBearerToken{
			TokenValue: "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...",
			Expiration: expiry,
			Principal:  "service-account@example.com",
			Extensions: map[string]string{
				"logicalCluster": "lkc-123",
				"identityPoolId": "pool-456",
			},
		}, nil
	})

	token, err := adapter.oauthTokenRefreshFn()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if token.TokenValue == "" {
		t.Error("expected token value to be set")
	}
	if token.Expiration.IsZero() {
		t.Error("expected expiration to be set")
	}
	if token.Principal == "" {
		t.Error("expected principal to be set")
	}
	if len(token.Extensions) != 2 {
		t.Errorf("expected 2 extensions, got %d", len(token.Extensions))
	}
}
