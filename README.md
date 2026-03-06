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
    Source     string         `json:"source"`     // Originating service
    Schema     string         `json:"schema"`     // Routing domain
    ResourceID string         `json:"resourceId"` // Primary entity ID
    Data       any            `json:"data"`       // Actual payload
    Metadata   map[string]any `json:"metadata"`   // Key-value headers
}
```

### 2. Routing Configuration
The `Router` maps logical events (Schema + EventType) to physical destinations and defines their delivery behavior.

```go
registry := event.SchemaRegistry{
    "order_domain": {
        "order.created": {
            QueueType:    "kafka",
            Destinations: []string{"prod.orders.created"},
            DLQPostfix:   event.Ptr(".failed"), // Physical topic: prod.orders.created.failed
        },
    },
}
router := event.NewStaticRouter(registry)
```

#### Loading from YAML
The registry is compatible with standard YAML/JSON tags.

**config.yaml**:
```yaml
order_domain:
  order.created:
    queue_type: "kafka"
    destinations: ["prod.orders.created"]
    dlq_postfix: ".failed"
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
    "order.created": {
      "queueType": "kafka",
      "destinations": ["prod.orders.created"],
      "dlqPostfix": ".failed"
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

kBroker := kafka.NewBroker(kafka.Config{
    Brokers: []string{"localhost:9092"},
    Writer: kafka.WriterConfig{ BatchSize: 100 },
})
```

#### RabbitMQ
```go
import "github.com/squall-chua/go-event-pubsub/pkg/broker/rabbitmq"

rBroker, _ := rabbitmq.NewBroker("amqp://guest:guest@localhost:5672/")
```

---

### Publishing Events
Publishing is non-blocking. Validation happens immediately, but network delivery occurs in the background.

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
    Schema:    "order_domain",
    Data:      map[string]any{"order_id": "123"},
}

if err := pub.Publish(ctx, evt); err != nil {
    // Initial validation or broker lookup failed
    log.Fatal(err)
}
```

---

### Subscribing to Events
Subscribers handle all event types within a specific schema context.

```go
sub := event.NewSubscriber("order_domain", router, brokers)

// Register a handler
sub.Subscribe("order.created", func(ctx context.Context, evt *event.Event) error {
    log.Printf("Processing Order: %v", evt.Data)
    
    // Returning an error here automatically triggers the DLQ
    return nil
})

// Start consumption (blocks until context is cancelled)
if err := sub.Start(ctx); err != nil {
    log.Fatal(err)
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
When an event fails (either background delivery retries are exhausted, or a subscriber handler returns an error), the event is wrapped and sent to the configured DLQ topic.

The DLQ message will have:
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
