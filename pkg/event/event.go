// Package event provides the core interfaces and data structures for the event-driven pub-sub system.
// It abstracts the underlying message brokers and provides a consistent API for publishing and subscribing to events.
package event

import (
	"context"
	"time"
)

// Event defines the standard payload for all messages sent through the system.
// It contains both the actual data payload and essential metadata for routing and diagnostics.
type Event struct {
	// EventId is a unique identifier for the event (typically a UUID).
	EventId string `json:"eventId"`
	// EventType is the name of the event (e.g., "user.created", "order.placed").
	EventType string `json:"eventType"`
	// EventTime is the UTC timestamp of when the event occurred.
	EventTime time.Time `json:"eventTime"`
	// Source identifies the service or system that originated the event.
	Source string `json:"source"`
	// Schema version or identifier used for routing and validation.
	Schema string `json:"schema"`
	// ResourceID is the ID of the primary resource associated with this event.
	ResourceID string `json:"resourceId"`
	// Data is the actual event payload. It must be JSON marshallable.
	Data any `json:"data"`
	// Metadata contains additional context like tracing headers, correlation IDs, or diagnostic info.
	Metadata map[string]any `json:"metadata"`
}

// EventHandler is the callback triggered when a Subscriber receives an event.
// Returning an error will trigger the Dead Letter Queue (DLQ) if configured.
type EventHandler func(ctx context.Context, evt *Event) error

// DLQFallbackHandler is a callback function invoked when a message fails to be delivered
// even to the Dead Letter Queue (e.g., if the broker is completely unreachable).
type DLQFallbackHandler func(ctx context.Context, evt *Event, dlqErr error)

// RetryConfig defines the parameters for the exponential backoff retry mechanism.
// It controls the frequency and duration of retry attempts when publishing to a broker fails.
type RetryConfig struct {
	// InitialInterval is the duration to wait before the first retry.
	InitialInterval time.Duration
	// MaxInterval is the upper bound on the backoff duration.
	MaxInterval time.Duration
	// MaxElapsedTime is the total maximum time to keep retrying before giving up and sending to DLQ.
	MaxElapsedTime time.Duration
}

// Publisher is responsible for routing and sending events to one or more destinations.
// A typical implementation will determine the routing based on a Router and send via a Broker.
//
// Example:
//
//	pub := event.NewPublisher(router, brokers, nil)
//	err := pub.Publish(ctx, &event.Event{ ... })
type Publisher interface {
	// Publish routes the event according to its schema and publishes it.
	// It is generally non-blocking, enqueuing the event for background delivery.
	Publish(ctx context.Context, evt *Event) error
}

// Subscriber is responsible for listening to events on designated topics.
// It manages the lifecycle of broker connections and dispatches events to registered handlers.
//
// Example:
//
//	sub := event.NewSubscriber("userSchema", router, brokers)
//	sub.Subscribe("user.created", func(ctx context.Context, evt *event.Event) error {
//	    log.Printf("User created: %s", evt.ResourceID)
//	    return nil
//	})
//	sub.Start(ctx)
type Subscriber interface {
	// Subscribe registers a callback for a specific event type based on the schema mapping.
	Subscribe(eventType string, handler EventHandler) error

	// Start begins listening and consuming messages. It blocks until the context is cancelled.
	Start(ctx context.Context) error
}

// Broker abstracts the actual messaging backend (Kafka, RabbitMQ, Memory, etc.).
// It handles the low-level transport of Event pointers.
type Broker interface {
	// Publish sends the event to the specified physical topic or queue.
	Publish(ctx context.Context, topic string, evt *Event) error

	// Consume registers a low-level handler for events on the specified topic.
	Consume(ctx context.Context, topic string, handler func(evt *Event) error) error
}
