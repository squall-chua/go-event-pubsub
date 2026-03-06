// Package memory provides an in-memory implementation of the event.Broker interface.
// It is primarily intended for local development, unit testing, and highly localized pub-sub needs.
package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

// Broker implements the event.Broker interface using internal Go maps and channels.
//
// Features:
//   - Local Dispatch: All events are dispatched synchronously to registered handlers within the local process.
//   - Non-Persistent: No messages are stored; if no one is listening when an event is published, it is dropped.
//   - Thread Safe: Uses RWMutex to allow concurrent publishing and dynamic subscription management.
//
// Example:
//
//	broker := memory.NewBroker()
//	broker.Consume(ctx, "local-topic", func(evt *event.Event) error {
//	    fmt.Println("Received:", evt.EventId)
//	    return nil
//	})
//	broker.Publish(ctx, "local-topic", &event.Event{EventId: "123"})
type Broker struct {
	mu          sync.RWMutex
	subscribers map[string][]func(*event.Event) error
}

// NewBroker creates and returns an initialized in-memory broker.
func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string][]func(*event.Event) error),
	}
}

// Publish iterates through all registered handlers for the given topic and calls them sequentially.
// It returns an error if any of the handlers fail.
func (b *Broker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	b.mu.RLock()
	handlers, ok := b.subscribers[topic]
	b.mu.RUnlock()

	if !ok {
		return nil // No subscribers, message dropped
	}

	for _, handler := range handlers {
		if err := handler(evt); err != nil {
			// In memory, we just return the first error for simplicity or log it
			return fmt.Errorf("handler error on topic %s: %w", topic, err)
		}
	}

	return nil
}

// Consume adds a handler function to the list of listeners for the specified topic.
func (b *Broker) Consume(ctx context.Context, topic string, handler func(*event.Event) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscribers[topic] = append(b.subscribers[topic], handler)
	return nil
}
