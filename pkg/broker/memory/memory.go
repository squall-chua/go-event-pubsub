// Package memory provides an in-memory implementation of the event.Broker interface.
// It is primarily intended for local development, unit testing, and highly localized pub-sub needs.
package memory

import (
	"context"
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
	publishCh   chan publishTask
}

type publishTask struct {
	topic string
	evt   *event.Event
}

// NewBroker creates and returns an initialized in-memory broker with a background dispatcher.
func NewBroker() *Broker {
	b := &Broker{
		subscribers: make(map[string][]func(*event.Event) error),
		publishCh:   make(chan publishTask, 1024), // Buffered to handle spikes
	}
	go b.dispatchLoop()
	return b
}

// Publish enqueues the event for delivery to all registered handlers for the given topic.
// It is non-blocking to the producer unless the internal buffer is full.
func (b *Broker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	select {
	case b.publishCh <- publishTask{topic: topic, evt: evt}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Broker) dispatchLoop() {
	for task := range b.publishCh {
		b.mu.RLock()
		handlers := b.subscribers[task.topic]
		b.mu.RUnlock()

		for _, handler := range handlers {
			go func(h func(*event.Event) error, e *event.Event) {
				_ = h(e)
			}(handler, task.evt)
		}
	}
}

// Consume adds a handler function to the list of listeners for the specified topic.
func (b *Broker) Consume(ctx context.Context, topic string, handler func(*event.Event) error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.subscribers[topic] = append(b.subscribers[topic], handler)
	return nil
}
