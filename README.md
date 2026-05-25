# llingr-adapter-kafka

[![CI](https://github.com/llingr/llingr-adapter-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/llingr/llingr-adapter-kafka/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/llingr/llingr-adapter-kafka.svg)](https://pkg.go.dev/github.com/llingr/llingr-adapter-kafka)
[![Go Report Card](https://goreportcard.com/badge/github.com/llingr/llingr-adapter-kafka)](https://goreportcard.com/report/github.com/llingr/llingr-adapter-kafka)
[![Tag](https://img.shields.io/github/v/tag/llingr/llingr-adapter-kafka)](https://github.com/llingr/llingr-adapter-kafka/tags)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Go Version](https://img.shields.io/github/go-mod/go-version/llingr/llingr-adapter-kafka)](go.mod)

Kafka adapter for [llingr-nexus](https://github.com/llingr/llingr-nexus)
consumers, built on [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go).

## Overview

This adapter implements `nexus.BrokerPort[*kafka.Message]` from
[llingr-nexus](https://github.com/llingr/llingr-nexus), while exposing the full
range of librdkafka configuration to consumer engines that consume the nexus contracts.

One adapter consumes from one topic; create multiple adapters for multiple topics.

## Requirements

This adapter wraps `librdkafka` via CGO, so the build environment must have:

- Go 1.24 or newer
- `librdkafka` system library installed (e.g. `apt-get install librdkafka-dev`,
  `brew install librdkafka`, or `apk add librdkafka-dev`)
- `CGO_ENABLED=1` (the Go default; pure-Go builds with `CGO_ENABLED=0` will fail)

## Installation

```bash
go get github.com/llingr/llingr-adapter-kafka
```

## Quick Start

```go
import (
    "log"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/llingr/llingr-adapter-kafka/kafkaadapter"
)

configMap := &kafka.ConfigMap{
    "bootstrap.servers": "localhost:9092",
    "group.id":          "my-consumer-group",
}

adapter, err := kafkaadapter.New(configMap)
if err != nil {
    log.Fatal(err)
}

// builder is a nexus.ConsumerBuilder[*kafka.Message] from whichever
// consumer engine you're using. Topic name comes from builder.TopicName().
consumer := adapter.CreateConsumer(builder)
if err := consumer.Subscribe(); err != nil {
    log.Fatal(err)
}
```

## All Configuration Options

See the [librdkafka configuration documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
for the complete list of available settings.

## ConfigMap Behaviour

The adapter validates and adjusts your ConfigMap at construction. Most settings
pass through unchanged to librdkafka. The exceptions:

**Enforced (panics on violation):**

- `enable.auto.commit` must be `false` (or unset) - auto-commit races with
  explicit offset management
- `enable.auto.offset.store` must be `false` (or unset) - same reason
- `isolation.level` must not be `read_uncommitted` - can cause duplicates by
  surfacing aborted-transaction writes, rarely a deliberate choice in
  idiomatic consumer code
- `go.events.channel.enable` must be `false` (or unset) - incompatible with
  this adapter's poll-loop design

**Warned (via `log.Printf` to stderr):**

- `session.timeout.ms` below 25000 ms - a value below this is fine for fast
  consumers, but the 25s floor covers the majority of slow-consumer use cases
  within a typical pod-termination grace period (~30s in Kubernetes). A
  timeout comfortably within that period lets in-flight processing and offset
  commits complete before the broker ejects the consumer during a rolling
  deployment. If 25 seconds isn't enough, it's worth a look at your handler
  design - that's a long time in compute terms.
- `max.poll.interval.ms` below 30000 ms - similar concern: if a handler is in
  flight when termination begins, `Poll()` can't fire until it returns. The
  30s floor accommodates most handler workloads within the typical
  pod-termination grace period.

All other settings pass through unchanged to librdkafka.

## Authentication

Authentication method depends on your broker configuration - check with your platform
team if unsure.

| Auth Method                            | Requires                                     | Section                           |
|----------------------------------------|----------------------------------------------|-----------------------------------|
| Username/password                      | username, password, CA cert                  | [SASL/SCRAM](#saslscram)          |
| Client certificates                    | CA cert, client cert, client key             | [mTLS](#mtls-client-certificates) |
| OAuth/OIDC (incl. Confluent Cloud SSO) | token endpoint URL, client ID, client secret | [OAUTHBEARER](#oauthbearer)       |
| AWS MSK                                | AWS credentials or IAM role                  | [MSK IAM](#msk-iam-aws)           |
| Confluent Cloud (API key)              | API key, API secret                          | [SASL/PLAIN](#saslplain)          |

All settings are configured via the `ConfigMap` passed to `New()`. Settings
pass through to librdkafka unchanged, except for the few enforced or warned
about under [ConfigMap Behaviour](#configmap-behaviour).

### SASL/SCRAM

Username/password authentication with secure hashing. Use `SCRAM-SHA-256` or
`SCRAM-SHA-512` depending on your broker configuration. Requires TLS (`ssl.ca.location`).

```go
configMap := &kafka.ConfigMap{
    "bootstrap.servers": "broker:9093",
    "group.id":          "my-group",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "SCRAM-SHA-256",
    "sasl.username":     "user",
    "sasl.password":     "pass",
    "ssl.ca.location":   "/path/to/ca.crt", // CA cert from your platform team
}
```

### SASL/PLAIN

Simple username/password (credentials sent in clear text, protected by TLS).

```go
configMap := &kafka.ConfigMap{
    "bootstrap.servers": "broker:9093",
    "group.id":          "my-group",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "PLAIN",
    "sasl.username":     "user",
    "sasl.password":     "pass",
    "ssl.ca.location":   "/path/to/ca.crt", // CA cert from your platform team
}
```

### OAUTHBEARER

JWT-based authentication for identity providers like Keycloak, Okta, or Confluent Cloud.
The callback is invoked on startup and whenever librdkafka needs a fresh token.

```go
import (
    "encoding/json"
    "log"
    "net/http"
    "net/url"
    "os"
    "strings"
    "time"
)

configMap := &kafka.ConfigMap{
    "bootstrap.servers": "broker:9094",
    "group.id":          "my-group",
    "security.protocol": "SASL_PLAINTEXT", // or SASL_SSL with ssl.ca.location
    "sasl.mechanism":    "OAUTHBEARER",
}

adapter, err := kafkaadapter.New(configMap)
if err != nil {
    log.Fatal(err)
}

// Token refresh callback - called on startup and when token nears expiry
adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
    // Client credentials flow (common for service-to-service)
    data := url.Values{}
    data.Set("grant_type", "client_credentials")
    data.Set("client_id", "my-service")
    data.Set("client_secret", os.Getenv("OAUTH_CLIENT_SECRET"))

    // Keycloak example - Okta, Azure AD, Auth0 etc. have different endpoint URLs
    client := &http.Client{Timeout: 30 * time.Second}
    resp, err := client.Post(
        "https://keycloak.example.com/realms/kafka/protocol/openid-connect/token",
        "application/x-www-form-urlencoded",
        strings.NewReader(data.Encode()))
    if err != nil {
        return kafka.OAuthBearerToken{}, err
    }
    defer resp.Body.Close()

    var result struct {
        AccessToken string `json:"access_token"`
        ExpiresIn   int    `json:"expires_in"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
        return kafka.OAuthBearerToken{}, err
    }

    return kafka.OAuthBearerToken{
        TokenValue: result.AccessToken,
        Expiration: time.Now().Add(time.Duration(result.ExpiresIn) * time.Second),
        Principal:  "my-service",
    }, nil
})

consumer := adapter.CreateConsumer(builder)
if err := consumer.Subscribe(); err != nil {
    log.Fatal(err)
}
```

The adapter handles `OAuthBearerTokenRefresh` events from librdkafka automatically.
If the callback returns an error, it calls `SetOAuthBearerTokenFailure()` to
schedule a retry (librdkafka defaults to 10s).

### TLS

Encrypt traffic and verify the broker's certificate. Use this when the broker
authenticates itself but the client is anonymous - typical for internal networks
where authorisation is handled by broker-side ACLs or network controls.

```go
configMap := &kafka.ConfigMap{
    "bootstrap.servers": "broker:9093",
    "group.id":          "my-group",
    "security.protocol": "SSL",
    "ssl.ca.location":   "/path/to/ca.crt",
}
```

### mTLS (Client Certificates)

Mutual TLS where the broker also verifies the client's identity.

```go
configMap := &kafka.ConfigMap{
    "bootstrap.servers":        "broker:9093",
    "group.id":                 "my-group",
    "security.protocol":        "SSL",
    "ssl.ca.location":          "/path/to/ca.crt",
    "ssl.certificate.location": "/path/to/client.crt",
    "ssl.key.location":         "/path/to/client.key",
}
```

### MSK IAM (AWS)

For Amazon MSK with IAM authentication, use OAUTHBEARER with
[aws-msk-iam-sasl-signer-go](https://github.com/aws/aws-msk-iam-sasl-signer-go)
to generate tokens.

```go
import signer "github.com/aws/aws-msk-iam-sasl-signer-go/signer"

configMap := &kafka.ConfigMap{
    "bootstrap.servers": "broker:9094",
    "group.id":          "my-group",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism":    "OAUTHBEARER",
    // MSK uses public CAs (Amazon Trust Services), so no ssl.ca.location needed
    // For private endpoints or custom CAs, add: "ssl.ca.location": "/path/to/ca.crt"
}

adapter, err := kafkaadapter.New(configMap)
if err != nil {
    log.Fatal(err)
}

adapter.SetOAuthTokenRefresh(func() (kafka.OAuthBearerToken, error) {
    token, _, err := signer.GenerateAuthToken(context.Background(), "us-east-1") // match your MSK cluster region
    if err != nil {
        return kafka.OAuthBearerToken{}, err
    }
    return kafka.OAuthBearerToken{
        TokenValue: token,
        Expiration: time.Now().Add(5 * time.Minute), // MSK tokens expire in ~5 min
        Principal:  "MSK",
    }, nil
})
```

## Advanced Configuration

### Rebalance Handling

Rebalance handling is automatic - the adapter detects whether your ConfigMap
has `go.application.rebalance.enable=true` and selects the matching policy
(poll-loop vs. subscribe-callback). To force a specific policy, pass it as a
variadic argument: `kafkaadapter.New(configMap, kafkaadapter.FromPoll)`.

### Full Client Control (NewCustom)

For complete control over client creation, use `NewCustom()` with an existing consumer:

```go
import (
    "log"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/llingr/llingr-adapter-kafka/kafkaadapter"
)

kafkaConsumer, err := kafka.NewConsumer(configMap)
if err != nil {
    log.Fatal(err)
}

adapter := kafkaadapter.NewCustom()
adapter.SetConsumer(kafkaConsumer)
consumer := adapter.CreateConsumer(builder)
```

## Bandwidth Telemetry

The adapter implements `nexus.BandwidthPort` for wire-level bandwidth metering.
Enable it by calling `WithBandwidthInterval` before `CreateConsumer`:

```go
import "time"

adapter, err := kafkaadapter.New(configMap)
if err != nil {
    log.Fatal(err)
}
adapter.WithBandwidthInterval(time.Minute) // collection cadence: 1s to 12h
consumer := adapter.CreateConsumer(builder)
```

You must also set `statistics.interval.ms` in the ConfigMap to match. This is
the librdkafka setting that controls how often stats events are emitted:

```go
configMap := &kafka.ConfigMap{
    "bootstrap.servers":      "broker:9092",
    "group.id":               "my-group",
    "statistics.interval.ms": 60000, // match WithBandwidthInterval
}
```

The adapter emits per-interval deltas, not cumulative totals. librdkafka
internally tracks cumulative byte and message counters from client start; the
adapter computes the difference since the previous stats event and emits only
the increment. Downstream consumers (such as [llingr-metrics-prometheus][1] and
other aggregators) can safely call `Add()` on each packet without double-counting.

[1]: https://github.com/llingr/llingr-metrics-prometheus

Byte counts reflect wire-level traffic as reported by librdkafka, which includes
Kafka protocol framing and record batch headers. This typically adds a small
overhead (under 1%) compared to raw payload sizes. Compression visibility
(CompressedBytes, UncompressedBytes) is not exposed through the bundled
librdkafka version's statistics interface and will be zero.

## Repository Layout

This repo is split into two Go modules:

```
llingr-adapter-kafka/
├── go.mod                     # kafkaadapter module - what consumers import
├── kafkaadapter/              # public API + unit tests
└── integration/
    ├── go.mod                 # separate module: testcontainers + Docker deps
    ├── simple_consumer_helper_test.go
    ├── simple_consumer_test.go
    └── kafka_integration_test.go
```

The integration tests live in their own module so the large testcontainers/Docker
dependency tree (around 40 transitive packages) stays out of `go.mod`. Anyone
running `go get github.com/llingr/llingr-adapter-kafka` pulls only the two
direct deps the runtime actually needs: `confluent-kafka-go` and `llingr-nexus`.

The `integration` module declares
`replace github.com/llingr/llingr-adapter-kafka => ../`, so its tests always
exercise the local source tree rather than a published version. The
`integration` module is never published.

## Development

```bash
# Unit tests only (fast, no Docker, kafkaadapter module)
make test

# Integration tests (requires Docker, integration module)
make integration

# Build, unit tests, and integration tests (the default target)
make
```

The kafkaadapter module's minimum Go version is 1.24. The integration module
requires Go 1.26.3.

## Licence

Apache-2.0 - see [LICENSE](./LICENSE) and [COPYRIGHT](./COPYRIGHT).
Contributions are governed by [CONTRIBUTING.md](./CONTRIBUTING.md).
