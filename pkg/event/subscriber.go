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

	type consumerJob struct {
		topic   string
		handler EventHandler
		broker  Broker
		config  *TopicConfig
	}

	// Validate all routes and brokers upfront before launching any goroutine.
	var jobs []consumerJob
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
				jobs = append(jobs, consumerJob{
					topic:   topic,
					handler: handler,
					broker:  b,
					config:  config,
				})
			}
		}
	}

	errChan := make(chan error, len(jobs))

	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(j consumerJob) {
			defer wg.Done()

			err := j.broker.Consume(ctx, j.topic, func(evt *Event) error {
				if err := j.handler(ctx, evt); err != nil {
					dlqEvt := *evt
					dlqEvt.EventType = evt.EventType + j.config.GetDLQEventTypePostfix()
					dlqEvt.EventTime = time.Now().UTC()
					dlqEvt.Data = evt // Keep original event as data

					if dlqEvt.Metadata == nil {
						dlqEvt.Metadata = make(map[string]any)
					}
					dlqEvt.Metadata["fail_reason"] = err.Error()
					dlqEvt.Metadata["original_destination"] = j.topic

					dlqTopic := j.topic + j.config.GetDLQPostfix()
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
