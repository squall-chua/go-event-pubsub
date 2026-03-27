---
name: go-event-pubsub
description: >
  Expert guide for the `go-event-pubsub` library — a lightweight, production-ready
  Go event-driven pub/sub framework with Kafka, RabbitMQ, and in-memory broker support.
  Use when working with this library: publishing events, subscribing to events, configuring
  routers/schemas, handling DLQ, testing with memory broker, or implementing custom brokers.
---

# go-event-pubsub Skill

## Overview

`go-event-pubsub` is a Go library that provides a **high-level, broker-agnostic abstraction** for
event-driven architectures. It unifies Kafka, RabbitMQ, and an in-memory broker behind a single
`Broker` interface, and adds production-grade features on top: non-blocking publishing, automatic
exponential-backoff retries, dead-letter queues (DLQ), and wildcard event routing.

**Module path**: `github.com/squall-chua/go-event-pubsub`

---

## How to Check Documentation

### 1. README (Primary Reference)

The `README.md` at the repository root is the canonical, always-up-to-date reference:

```bash
# From repository root
cat README.md
```

It covers: installation, core concepts, all usage examples, DLQ semantics, and the examples index.

### 2. Go Package Documentation (pkg.go.dev)

After adding the dependency, browse API docs online:

```text
https://pkg.go.dev/github.com/squall-chua/go-event-pubsub
```

Or generate docs locally with:

```bash
go doc ./pkg/event/...          # Core event types and Publisher/Subscriber
go doc ./pkg/broker/kafka/...   # Kafka broker
go doc ./pkg/broker/rabbitmq/.. # RabbitMQ broker
go doc ./pkg/broker/memory/...  # In-memory broker (for testing)
```

### 3. Runnable Examples (examples/ directory)

The `examples/` directory contains self-contained, runnable programs for every major feature:

| Example | Path | What it demonstrates |
| --- | --- | --- |
| Basic In-Memory | `examples/basic_memory/main.go` | Simplest publish/subscribe loop |
| Kafka Integration | `examples/kafka_producer_consumer/main.go` | Performance tuning, consumer groups |
| RabbitMQ Integration | `examples/rabbitmq_producer_consumer/main.go` | Reliable AMQP usage |
| DLQ Diagnostics | `examples/dlq_diagnostics/main.go` | Failure wrapping & metadata inspection |
| DLQ Recovery | `examples/dlq_recovery/main.go` | Reprocessing events via `DLQProcessor` |
| Publisher Fallback | `examples/dlq_fallback/main.go` | Emergency handling for unreachable brokers |
| Subscriber Fallback | `examples/subscriber_dlq_fallback/main.go` | Failed DLQ routing during consumption |
| Custom Broker | `examples/custom_broker/main.go` | Extending with a custom messaging backend |

Run any example:

```bash
go run examples/basic_memory/main.go
```

### 4. Source Code

Core logic lives under `pkg/`:

```text
pkg/
├── broker/
│   ├── kafka/      # Kafka broker implementation
│   ├── rabbitmq/   # RabbitMQ broker implementation
│   └── memory/     # In-memory broker (perfect for tests)
└── event/
    ├── event.go          # Event struct, SchemaRegistry, TopicConfig
    ├── router.go         # StaticRouter and routing resolution
    ├── publisher.go      # Publisher with worker pool & retries
    ├── subscriber.go     # Subscriber with wildcard dispatch
    └── dlq_processor.go  # DLQProcessor for recovery
```

---

## Installation

```bash
go get github.com/squall-chua/go-event-pubsub
```

---

## Core Concepts Reference

### The Event Struct

```go
type Event struct {
    EventId    string         `json:"eventId"`    // Unique tracking ID (use uuid)
    EventType  string         `json:"eventType"`  // e.g., "order.created"
    EventTime  time.Time      `json:"eventTime"`  // UTC occurrence time
    User       string         `json:"user"`       // Triggering user ID
    Source     string         `json:"source"`     // Originating service name
    Schema     string         `json:"schema"`     // Routing domain key (must match registry)
    ResourceID string         `json:"resourceId"` // Primary entity ID
    Data       any            `json:"data"`       // Actual payload (any serialisable type)
    Metadata   map[string]any `json:"metadata"`   // Key-value headers
}
```

### SchemaRegistry / Router

The `SchemaRegistry` maps logical schemas and event types to physical broker destinations.
Fields cascade from schema level down to individual event configs.

```go
registry := event.SchemaRegistry{
    "order_domain": {
        QueueType:    "kafka",            // default broker for all events in domain
        Destinations: []string{"orders"}, // default topics/queues
        DLQPostfix:   ".failed",          // appended to destination for DLQ topic
        Events: map[string]event.TopicConfig{
            "order.created": {},           // inherits schema defaults
            "order.internal.*": {          // wildcard: matches order.internal.foo, etc.
                QueueType:    "memory",
                Destinations: []string{"internal-logs"},
            },
        },
    },
}
router := event.NewStaticRouter(registry)
```

**Wildcard rules:**

- `order.*` — prefix match: any event starting with `order.`
- `*` — global match: every event type in the schema

### Loading Registry from YAML

```go
import "gopkg.in/yaml.v3"

var registry event.SchemaRegistry
data, _ := os.ReadFile("config.yaml")
_ = yaml.Unmarshal(data, &registry)
router := event.NewStaticRouter(registry)
```

---

## Usage Patterns

### 1. Publisher Setup

```go
import (
    "github.com/squall-chua/go-event-pubsub/pkg/event"
    "github.com/squall-chua/go-event-pubsub/pkg/broker/kafka"
)

// 1a. Initialise broker(s)
kBroker, err := kafka.NewBroker(kafka.Config{
    Brokers: []string{"localhost:9092"},
    Writer:  kafka.WriterConfig{BatchSize: 100},
})
if err != nil {
    log.Fatal(err)
}

// 1b. Configure publisher
cfg := &event.PublisherConfig{
    Workers:    10,   // concurrent delivery goroutines
    BufferSize: 500,  // internal task buffer
    RetryConfig: &event.RetryConfig{
        InitialInterval: 500 * time.Millisecond,
        MaxElapsedTime:  30 * time.Second,
    },
    DLQFallbackHandler: func(ctx context.Context, evt *event.Event, dlqErr error) {
        log.Printf("EMERGENCY: DLQ unavailable for event %s: %v", evt.EventId, dlqErr)
    },
}

// 1c. Create publisher
brokers := map[string]event.Broker{"kafka": kBroker}
pub := event.NewPublisher(router, brokers, cfg)
defer pub.Close() // drains pending tasks before shutdown
```

### 2. Publishing an Event

```go
evt := &event.Event{
    EventId:   uuid.NewString(),
    EventType: "order.created",
    Schema:    "order_domain",
    User:      "user_123",
    Data:      map[string]any{"order_id": "abc"},
}

// Publish is non-blocking. Returns error only for routing failures (synchronous).
if err := pub.Publish(ctx, evt); err != nil {
    log.Printf("routing error: %v", err) // e.g., unregistered EventType
}
```

### 3. Subscriber Setup and Graceful Shutdown

```go
sub := event.NewSubscriber(router, brokers, nil)

sub.Subscribe("order_domain", "order.*", func(ctx context.Context, evt *event.Event) error {
    log.Printf("Received %s for user %s", evt.EventType, evt.User)
    return nil // return non-nil to trigger DLQ for this event
})

// Start is non-blocking — validates config synchronously, runs consumers in background.
ctx, cancel := context.WithCancel(context.Background())
errCh, err := sub.Start(ctx)
if err != nil {
    log.Fatal(err) // config error: nothing started
}

// Optional: watch for runtime consumer errors
go func() {
    for err := range errCh {
        log.Printf("consumer error: %v", err)
    }
}()

// Graceful shutdown
cancel()
for err := range errCh { // blocks until all consumer goroutines exit
    log.Printf("shutdown error: %v", err)
}
```

### 4. Multiple Subscribers with errgroup

```go
import "golang.org/x/sync/errgroup"

g, ctx := errgroup.WithContext(mainCtx)
for _, s := range []event.Subscriber{sub1, sub2} {
    sub := s
    g.Go(func() error {
        errCh, err := sub.Start(ctx)
        if err != nil {
            return err
        }
        for err := range errCh {
            log.Printf("consumer error: %v", err)
        }
        return nil
    })
}
if err := g.Wait(); err != nil {
    log.Printf("system stopped: %v", err)
}
```

### 5. Testing with the In-Memory Broker

```go
import "github.com/squall-chua/go-event-pubsub/pkg/broker/memory"

func TestMyLogic(t *testing.T) {
    memBroker := memory.NewBroker()
    brokers := map[string]event.Broker{"memory": memBroker}

    pub := event.NewPublisher(router, brokers, nil)
    sub := event.NewSubscriber(router, brokers, nil)

    // ... test normally, no external services needed
}
```

### 6. Dead Letter Queue (DLQ)

Configure `DLQPostfix` in the schema registry. When delivery fails after all retries:

- The event is wrapped and sent to `<destination><DLQPostfix>` (e.g., `orders.failed`).
- The DLQ event's `Metadata` contains:
  - `fail_reason` — the error string
  - `original_destination` — the intended topic

**Recovery with DLQProcessor:**

```go
processor := event.NewDLQProcessor(broker, pub)

err := processor.Process(ctx, "orders.failed", func(evt *event.Event) bool {
    // Return true to requeue this event, false to skip it
    reason, _ := evt.Metadata["fail_reason"].(string)
    return strings.Contains(reason, "connection timeout")
})
```

### 7. Implementing a Custom Broker

Implement the `event.Broker` interface and register it in the brokers map:

```go
// See examples/custom_broker/main.go for a full reference implementation
type MyBroker struct{}

func (b *MyBroker) Publish(ctx context.Context, topic string, payload []byte) error { ... }
func (b *MyBroker) Subscribe(ctx context.Context, topic string, handler func([]byte) error) error { ... }

brokers := map[string]event.Broker{"my_broker": &MyBroker{}}
```

---

## Key Behaviours to Know

| Behaviour | Detail |
| --- | --- |
| `Publish()` error | Only returned for **routing failures** (sync). Delivery failures happen in background. |
| `Start()` error | Only returned for **config/routing errors** (sync). Runtime errors go to `errCh`. |
| Thread safety | Events are deep-cloned before being enqueued — zero data races. |
| DLQ gating | DLQ only activates when `DLQPostfix` is set. Without it, failed events are dropped/logged. |
| Retry policy | Uses `cenkalti/backoff/v5` exponential backoff. Configure via `RetryConfig`. |
| Wildcard routing | Prefix (`domain.*`) and global (`*`) wildcards supported in both routers and subscribers. |

---

## Quick Reference: Common Errors

| Error | Cause | Fix |
| --- | --- | --- |
| `event type not registered` | `EventType` not in schema registry | Add event type to `SchemaRegistry.Events` |
| `broker not found` | `QueueType` key not in brokers map | Add the broker to the `brokers` map |
| `no brokers configured` (Kafka) | Empty `kafka.Config.Brokers` | Provide at least one broker address |
| subscriber handler returns error | Business logic failure | Implement retry or DLQ handling |
