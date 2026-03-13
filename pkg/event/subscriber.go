package event

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// DefaultSubscriber is the standard implementation of the Subscriber interface.
// It manages the consumption of events across one or more schemas, routing them
// from brokers to registered Go handlers.
//
// Key Features:
//   - Multi-Schema: A single subscriber can handle event types from different schemas.
//   - Topic Abstraction: Automatically maps (schema, eventType) pairs to physical topics using the provided Router.
//   - Automatic DLQ: If a registered EventHandler returns an error, the event is automatically enriched with error metadata and moved to the configured Dead Letter Queue.
//   - Concurrent Consumption: Uses goroutines to consume from multiple topics simultaneously.
type DefaultSubscriber struct {
	brokers            map[string]Broker
	router             Router
	// handlers is keyed by schema, then by eventType.
	handlers           map[string]map[string]EventHandler
	mu                 sync.RWMutex
	dlqFallbackHandler DLQFallbackHandler
}

// SubscriberConfig defines behavioral settings for the DefaultSubscriber.
type SubscriberConfig struct {
	// DLQFallbackHandler is an optional hook to handle events that failed DLQ delivery.
	// If nil, failures will be logged to the standard logger.
	DLQFallbackHandler DLQFallbackHandler
}

// NewSubscriber creates a new DefaultSubscriber that can handle events across multiple schemas.
//
// Example:
//
//	sub := event.NewSubscriber(router, brokers, nil)
//	sub.Subscribe("order_domain", "order.placed", func(ctx context.Context, evt *event.Event) error {
//	    order := evt.Data.(*Order)
//	    return processOrder(order)
//	})
//	sub.Subscribe("payment_domain", "payment.completed", func(ctx context.Context, evt *event.Event) error {
//	    return processPayment(ctx, evt)
//	})
//
//	// Blocks until context cancelled or fatal error
//	if err := sub.Start(ctx); err != nil {
//	    log.Fatal(err)
//	}
func NewSubscriber(router Router, brokers map[string]Broker, config *SubscriberConfig) Subscriber {
	cfg := config
	if cfg == nil {
		cfg = &SubscriberConfig{}
	}

	fallback := cfg.DLQFallbackHandler
	if fallback == nil {
		fallback = func(ctx context.Context, evt *Event, dlqErr error) {
			// By default, log critical failure
			fmt.Printf("[EventLib] CRITICAL: Subscriber failed to write to DLQ for event %s. Error: %v\n",
				evt.EventId, dlqErr)
		}
	}

	return &DefaultSubscriber{
		brokers:            brokers,
		router:             router,
		handlers:           make(map[string]map[string]EventHandler),
		dlqFallbackHandler: fallback,
	}
}

// Subscribe registers an EventHandler for a specific (schema, eventType) pair.
// The same subscriber can register handlers for event types across different schemas.
// Topic mapping is performed automatically during Start() using the router.
func (s *DefaultSubscriber) Subscribe(schema, eventType string, handler EventHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.handlers[schema]; !ok {
		s.handlers[schema] = make(map[string]EventHandler)
	}
	s.handlers[schema][eventType] = handler
	return nil
}

// Start begins the consumption loop for all subscribed event types in the background.
//
// It validates routing and broker availability synchronously, returning an error immediately
// if any configuration is invalid. Consumption goroutines are then launched in the background.
//
// The returned channel receives fatal errors from consumer goroutines at runtime.
// The channel is closed once all consumers have exited (context cancelled or all done).
//
// If a handler returns an error, it does NOT stop the subscriber. Instead, the failed event
// is routed to the Dead Letter Queue (DLQ) and consumption continues.
//
// Example:
//
//	errCh, err := sub.Start(ctx)
//	if err != nil {
//	    log.Fatal(err) // config/routing error
//	}
//	go func() {
//	    for err := range errCh {
//	        log.Printf("subscriber error: %v", err)
//	    }
//	}()
func (s *DefaultSubscriber) Start(ctx context.Context) (<-chan error, error) {
	s.mu.RLock()
	handlers := s.handlers
	s.mu.RUnlock()

	type topicEntry struct {
		handler EventHandler
		config  *TopicConfig
	}

	type dispatcherJob struct {
		topic   string
		broker  Broker
		entries map[string]map[string]topicEntry // schema -> eventType -> entry
	}

	// Group handlers and configs by topic and broker to support multiplexing multiple
	// event types from the same destination.
	jobMap := make(map[string]*dispatcherJob)

	for schema, eventHandlers := range handlers {
		for eventType, handler := range eventHandlers {
			config, err := s.router.RouteFor(schema, eventType)
			if err != nil {
				return nil, fmt.Errorf("failed to route event type %s (schema: %s): %w", eventType, schema, err)
			}

			b, ok := s.brokers[config.QueueType]
			if !ok {
				return nil, fmt.Errorf("no broker configured for queue type: %s", config.QueueType)
			}

			for _, topic := range config.Destinations {
				jobKey := config.QueueType + ":" + topic
				job, ok := jobMap[jobKey]
				if !ok {
					job = &dispatcherJob{
						topic:   topic,
						broker:  b,
						entries: make(map[string]map[string]topicEntry),
					}
					jobMap[jobKey] = job
				}

				if _, ok := job.entries[schema]; !ok {
					job.entries[schema] = make(map[string]topicEntry)
				}
				job.entries[schema][eventType] = topicEntry{
					handler: handler,
					config:  config,
				}
			}
		}
	}

	errChan := make(chan error, len(jobMap))

	var wg sync.WaitGroup
	for _, job := range jobMap {
		wg.Add(1)
		go func(j *dispatcherJob) {
			defer wg.Done()

			err := j.broker.Consume(ctx, j.topic, func(evt *Event) error {
				// Multiplex events to the correct specific handler based on schema and event type.
				schemaHandlers, ok := j.entries[evt.Schema]
				if !ok {
					return nil // Ignore events from other schemas sharing the topic
				}

				entry, ok := schemaHandlers[evt.EventType]
				if !ok {
					return nil // Ignore events from other types sharing the topic
				}

				if err := entry.handler(ctx, evt); err != nil {
					if entry.config.DLQPostfix == nil {
						return fmt.Errorf("handler failed (DLQ disabled): %w", err)
					}

					dlqEvt := *evt
					dlqEvt.EventType = evt.EventType + entry.config.GetDLQEventTypePostfix()
					dlqEvt.EventTime = time.Now().UTC()
					dlqEvt.Data = evt // Keep original event as data

					if dlqEvt.Metadata == nil {
						dlqEvt.Metadata = make(map[string]any)
					}
					dlqEvt.Metadata["fail_reason"] = err.Error()
					dlqEvt.Metadata["original_destination"] = j.topic

					dlqTopic := j.topic + entry.config.GetDLQPostfix()
					if err := j.broker.Publish(ctx, dlqTopic, &dlqEvt); err != nil {
						s.dlqFallbackHandler(ctx, evt, err)
					}
					return fmt.Errorf("handler failed: %w", err)
				}
				return nil
			})
			if err != nil {
				errChan <- fmt.Errorf("consumer failed on topic %s: %w", j.topic, err)
			}
		}(job)
	}

	// Close errChan once all goroutines finish so range loops terminate.
	go func() {
		wg.Wait()
		close(errChan)
	}()

	return errChan, nil
}
