package event

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DefaultSubscriber is the standard implementation of the Subscriber interface.
// It manages the consumption of events for a specific schema, routing them from brokers to registered Go handlers.
//
// Key Features:
//   - Schema Scoped: One subscriber typically handles all event types within a single domain/schema.
//   - Topic Abstraction: Automatically maps EventTypes to physical topics using the provided Router.
//   - Automatic DLQ: If a registered EventHandler returns an error, the event is automatically enriched with error metadata and moved to the configured Dead Letter Queue.
//   - Concurrent Consumption: Uses goroutines to consume from multiple topics simultaneously.
type DefaultSubscriber struct {
	brokers  map[string]Broker
	router   Router
	schema   string
	handlers map[string]EventHandler
	mu       sync.RWMutex
}

// NewSubscriber creates a new DefaultSubscriber tied to a specific schema.
//
// Example:
//
//	sub := event.NewSubscriber("order_domain", router, brokers)
//	sub.Subscribe("order.placed", func(ctx context.Context, evt *event.Event) error {
//	    order := evt.Data.(*Order)
//	    return processOrder(order)
//	})
//
//	// Blocks until context cancelled or fatal error
//	if err := sub.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
func NewSubscriber(schema string, router Router, brokers map[string]Broker) *DefaultSubscriber {
	return &DefaultSubscriber{
		brokers:  brokers,
		router:   router,
		schema:   schema,
		handlers: make(map[string]EventHandler),
	}
}

// Subscribe registers an EventHandler for a specific event type.
// Topic mapping is performed automatically during Start() using the router.
func (s *DefaultSubscriber) Subscribe(eventType string, handler EventHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[eventType] = handler
	return nil
}

// Start begins the consumption loop for all subscribed event types.
//
// It resolves routing for each event type, connects to the appropriate brokers, and spawns
// background goroutines to listen on the physical topics.
//
// This method blocks until:
//  1. The passed context is cancelled (clean exit).
//  2. An underlying broker consumer fails fatally.
//
// If a handler returns an error, Start() does NOT exit. Instead, it routes the failed event
// to the Dead Letter Queue (DLQ) and continues consuming.
func (s *DefaultSubscriber) Start(ctx context.Context) error {
	s.mu.RLock()
	handlers := s.handlers
	s.mu.RUnlock()

	var wg sync.WaitGroup
	errChan := make(chan error, len(handlers))

	for eventType, handler := range handlers {
		config, err := s.router.RouteFor(s.schema, eventType)
		if err != nil {
			return fmt.Errorf("failed to route event type %s: %w", eventType, err)
		}

		b, ok := s.brokers[config.QueueType]
		if !ok {
			return fmt.Errorf("no broker configured for queue type: %s", config.QueueType)
		}

		for _, topic := range config.Destinations {
			wg.Add(1)
			go func(topic string, h EventHandler, brk Broker, cfg *TopicConfig) {
				defer wg.Done()

				err := brk.Consume(ctx, topic, func(evt *Event) error {
					if err := h(ctx, evt); err != nil {
						dlqEvt := *evt
						dlqEvt.EventType = evt.EventType + cfg.GetDLQEventTypePostfix()
						dlqEvt.EventTime = time.Now().UTC()
						dlqEvt.Data = evt // Keep original event as data

						if dlqEvt.Metadata == nil {
							dlqEvt.Metadata = make(map[string]any)
						}
						dlqEvt.Metadata["fail_reason"] = err.Error()
						dlqEvt.Metadata["original_destination"] = topic

						dlqTopic := topic + cfg.GetDLQPostfix()
						_ = brk.Publish(ctx, dlqTopic, &dlqEvt)
						return fmt.Errorf("handler failed: %w", err)
					}

					return nil
				})
				if err != nil {
					errChan <- fmt.Errorf("consumer failed on topic %s: %w", topic, err)
				}
			}(topic, handler, b, config)
		}
	}

	// Wait for context cancellation or fatal errors
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errChan:
		return err
	case <-done:
		return nil
	}
}
