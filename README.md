# Go Event PubSub

A lightweight, production-ready Go library for building event-driven architectures. It provides a high-level abstraction over popular message brokers like Kafka and RabbitMQ, featuring background delivery, automatic retries with backoff, and robust Dead Letter Queue (DLQ) support.

## Key Features

- **Standardized Payloads**: Uses a uniform `Event` struct for all communications, ensuring consistency across services.
- **Non-Blocking Publishing**: Integrated worker pool and task channel for asynchronous event delivery.
- **Robust Retries**: Built-in exponential backoff (v5) for handling transient broker failures.
- **Advanced Routing**: Decouples application logic from physical topics using a schema-based Router.
- **Observability in DLQ**: Automatically routes failed messages to DLQs with full diagnostic metadata (`fail_reason`, `original_destination`).
- **Multiple Backends**: Pluggable support for Kafka, RabbitMQ, and an In-Memory broker for testing.
- **Thread Safe**: Automatic deep cloning of event state ensuring zero data races between callers and background workers.

---

## Installation

```bash
go get github.com/squall-chua/go-event-pubsub
```

---

## Core Concepts

### 1. The Event Struct

Every message in the system is an `Event`. It includes standard fields for correlation and traceability.

```go
type Event struct {
    EventId    string         `json:"eventId"`    // Unique tracking ID
    EventType  string         `json:"eventType"`  // e.g., "order.created"
    EventTime  time.Time      `json:"eventTime"`  // UTC occurrence time
    User       string         `json:"user"`       // Triggering user ID
    Source     string         `json:"source"`     // Originating service
    Schema     string         `json:"schema"`     // Routing domain
    ResourceID string         `json:"resourceId"` // Primary entity ID
    Data       any            `json:"data"`       // Actual payload
    Metadata   map[string]any `json:"metadata"`   // Key-value headers
}
```

### 2. Routing Configuration
The `Router` maps logical events (Schema + EventType) to physical destinations and defines their delivery behavior. Destinations can be defined at the schema level to provide a default for all events.

```go
registry := event.SchemaRegistry{
    "order_domain": {
        QueueType:    "kafka",             // Schema-level default broker
        Destinations: []string{"orders"},  // Schema-level default destinations
        DLQPostfix:   ".failed",          // Schema-level default DLQ postfix
        Events: map[string]event.TopicConfig{
            "order.created": {
                // Inherits QueueType, Destinations, and DLQPostfix
            },
            "order.internal.*": { // Wildcard for all internal sub-events
                QueueType:    "memory", // Explicit override
                Destinations: []string{"internal-logs"}, // Explicit override
            },
        },
    },
}
router := event.NewStaticRouter(registry)
```

#### Wildcard Event Types

The `Router` and `Subscriber` support simple prefix-based wildcards using the `*` suffix:

- **Prefix Match**: `domain.*` matches `domain.created`, `domain.deleted`, etc.
- **Global Match**: `*` matches every event type within the schema.

This is particularly useful for building domain-wide event listeners or routing related events to the same topic without explicit registration for every single subtype.

#### Loading from YAML
The registry is compatible with standard YAML/JSON tags.

**config.yaml**:
```yaml
order_domain:
  queue_type: "kafka"
  destinations: ["orders"] # Default for all events in this schema
  dlq_postfix: ".failed"
  events:
    order.created:
      # Automatically inherits from order_domain
    order.internal:
      queue_type: "memory"
      destinations: ["internal-logs"] # Specific override
```

**Go**:
```go
import "gopkg.in/yaml.v3"

var registry event.SchemaRegistry
_ = yaml.Unmarshal(yamlData, &registry)
router := event.NewStaticRouter(registry)
```

**config.json**:
```json
{
  "order_domain": {
    "queueType": "kafka",
    "destinations": ["orders"],
    "dlqPostfix": ".failed",
    "events": {
      "order.created": {},
      "order.internal": {
        "queueType": "memory",
        "destinations": ["internal-logs"]
      }
    }
  }
}
```

---

## Usage Examples

### Initializing Brokers

#### Kafka
```go
import "github.com/squall-chua/go-event-pubsub/pkg/broker/kafka"

kBroker, err := kafka.NewBroker(kafka.Config{
    Brokers: []string{"localhost:9092"},
    Writer: kafka.WriterConfig{ BatchSize: 100 },
})
if err != nil {
    log.Fatal(err) // e.g. no brokers configured
}
```

#### RabbitMQ

```go
import "github.com/squall-chua/go-event-pubsub/pkg/broker/rabbitmq"

rBroker, _ := rabbitmq.NewBroker("amqp://guest:guest@localhost:5672/")
```

---

### Publishing Events

Publishing is non-blocking. **Validation happens immediately during the `Publish` call.** If the event type is not registered in the router, the call will return an error synchronously.

```go
// 1. Configure the publisher
pubCfg := &event.PublisherConfig{
    Workers:    10,  // Concurrent delivery units
    BufferSize: 500, // Internal queue size
    RetryConfig: &event.RetryConfig{
        InitialInterval: 500 * time.Millisecond,
        MaxElapsedTime:  30 * time.Second,
    },
}

// 2. Create the publisher
brokers := map[string]event.Broker{
    "kafka": kBroker,
}
pub := event.NewPublisher(router, brokers, pubCfg)
defer pub.Close() // Wait for pending tasks before shutdown

// 3. Publish
evt := &event.Event{
    EventId:   uuid.NewString(),
    EventType: "order.created",
    User:      "user_123",
    Schema:    "order_domain",
    Data:      map[string]any{"order_id": "123"},
}

// Publish returns an error if routing fails (e.g. unregistered EventType)
if err := pub.Publish(ctx, evt); err != nil {
    log.Printf("Rejected: %v", err)
}
```

---

### Subscribing to Events

Subscribers handle all event types within a specific schema context. `Start()` is **non-blocking** — it validates routing synchronously, launches consumer goroutines in the background, and returns immediately.

```go
sub := event.NewSubscriber(router, brokers, nil)

// Register a handler with a wildcard
sub.Subscribe("order_domain", "order.*", func(ctx context.Context, evt *event.Event) error {
    log.Printf("Received %s event for user %s", evt.EventType, evt.User)
    return nil
})

// Start is non-blocking: validates config synchronously, runs consumers in the background.
errCh, err := sub.Start(ctx)
if err != nil {
    // Config error: bad routing or missing broker — nothing has been started.
    log.Fatal(err)
}

// Optionally watch for fatal runtime consumer errors
go func() {
    for err := range errCh {
        log.Printf("subscriber runtime error: %v", err)
    }
}()
```

#### Graceful Shutdown

To wait for all consumer goroutines to finish before exiting, drain the error channel after cancelling the context — it is closed once all goroutines have exited.

```go
ctx, cancel := context.WithCancel(context.Background())
errCh, err := sub.Start(ctx)
if err != nil {
    log.Fatal(err)
}

// ... wait for SIGTERM ...

cancel() // propagate shutdown to all consumer goroutines

for err := range errCh { // blocks until all goroutines have exited
    log.Printf("subscriber error during shutdown: %v", err)
}
log.Println("subscriber fully stopped")
```

##### Multiple Subscribers
For complex applications with many subscribers, we recommend using `golang.org/x/sync/errgroup`. It provides a unified way to wait for all background tasks and handle fatal errors.

```go
import "golang.org/x/sync/errgroup"

// Create a group and derived context
g, ctx := errgroup.WithContext(mainCtx)

subs := []event.Subscriber{sub1, sub2}

for _, s := range subs {
    sub := s // capture loop var
    g.Go(func() error {
        errCh, err := sub.Start(ctx)
        if err != nil {
            return err
        }

        // Draining errCh ensures we wait for all internal consumers to finish
        for err := range errCh {
            log.Printf("Consumer runtime error: %v", err)
            // Note: In an errgroup, returning an error here would cancel all other subscribers.
            // Only return the error if you want a complete system stop.
        }
        return nil
    })
}

// Blocks until all subscribers have stopped or one returned a fatal error
if err := g.Wait(); err != nil {
    log.Printf("System stopped with error: %v", err)
}
```

---

### Testing with Memory Broker
The `memory` package provides a blazing-fast, local implementation perfect for unit tests.

```go
import "github.com/squall-chua/go-event-pubsub/pkg/broker/memory"

func TestMyLogic(t *testing.T) {
    memBroker := memory.NewBroker()
    // ... use exactly like the production brokers
}
```

---

## Dead Letter Queue (DLQ)

When an event fails (either background delivery retries are exhausted, or a subscriber handler returns an error), the event is wrapped and sent to the configured DLQ topic **only if `DLQPostfix` is specified in the routing configuration**.

If `DLQPostfix` is omitted:
- **Publishers** will log a warning and drop the event after all retries fail.
- **Subscribers** will return an error from the internal dispatcher and the event will be dropped (or retried by the broker depending on broker-specific ack settings).

When enabled, the DLQ message will have:
- **EventType**: Original event type + postfix (e.g., `order.created.failed`).
- **Data**: The full original event.
- **Metadata**:
  - `fail_reason`: The error string describing the failure.
  - `original_destination`: Where the event was supposed to go.

### Recovery from DLQ

The library provides a `DLQProcessor` to help automate the recovery of failed events. It unwraps the original event, strips failure metadata, and republishes it back into the main pipeline.

```go
processor := event.NewDLQProcessor(broker, pub)

// Process all events from a specific DLQ topic
err := processor.Process(ctx, "orders-topic.failed", func(evt *event.Event) bool {
    // Optional filter: only reprocess transient errors
    reason, _ := evt.Metadata["fail_reason"].(string)
    return strings.Contains(reason, "connection timeout")
})
```

### Unreachable DLQ Fallback

In extreme cases (e.g., the broker cluster is completely down), even publishing to the DLQ might fail. To prevent data loss, both `PublisherConfig` and `SubscriberConfig` provide a `DLQFallbackHandler` hook.

```go
// For Publisher
config := &event.PublisherConfig{
    DLQFallbackHandler: func(ctx context.Context, evt *event.Event, dlqErr error) {
        log.Printf("EMERGENCY: Broker down. Event: %s", evt.EventId)
    },
}

// For Subscriber
config := &event.SubscriberConfig{
    DLQFallbackHandler: func(ctx context.Context, evt *event.Event, dlqErr error) {
        // Handle failed subscriber DLQ moves here
    },
}
```
If no handler is provided, the library defaults to logging a highly visible error to standard out.
---

## Examples

The library includes several runnable examples under the `examples/` directory:

- [Basic In-Memory](examples/basic_memory/main.go): Simplest loop.
- [Kafka Integration](examples/kafka_producer_consumer/main.go): Performance tuning and groups.
- [RabbitMQ Integration](examples/rabbitmq_producer_consumer/main.go): Reliable AMQP usage.
- [DLQ Diagnostics](examples/dlq_diagnostics/main.go): Failure wrapping and metadata inspection.
- [DLQ Recovery](examples/dlq_recovery/main.go): Reprocessing events using the `DLQProcessor`.
- [Publisher Fallback](examples/dlq_fallback/main.go): Emergency handling for unreachable brokers.
- [Subscriber Fallback](examples/subscriber_dlq_fallback/main.go): Emergency handling for failed DLQ routing during consumption.
- [Custom Broker](examples/custom_broker/main.go): How to extend the library with your own messaging backend.

To run an example:
```bash
go run examples/basic_memory/main.go
```

---

## License
MIT
