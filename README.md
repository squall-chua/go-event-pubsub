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

---

## License
MIT
