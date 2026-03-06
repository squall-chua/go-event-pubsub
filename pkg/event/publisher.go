package event

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"
)

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

// PublisherConfig defines the overall configuration for the background publisher.
type PublisherConfig struct {
	// RetryConfig specifies the backoff settings. If nil, production defaults are used.
	RetryConfig *RetryConfig
	// Workers is the number of concurrent background goroutines processing events. Defaults to 1.
	Workers int
	// BufferSize is the capacity of the internal task channel. Defaults to 100.
	BufferSize int
}

type publishTask struct {
	evt    *Event
	config *TopicConfig
	broker Broker
}

// DefaultPublisher is the standard implementation of the Publisher interface.
// It provides non-blocking event publishing by using an internal worker pool and task channel.
//
// Key Features:
//   - Synchronous Validation: Routing and broker lookup happen immediately in Publish().
//   - Asynchronous Delivery: Actual network calls to brokers happen in background worker goroutines.
//   - Retries with Backoff: Integrated exponential backoff for transient failures.
//   - Dead Letter Queue (DLQ): Automatically routes events to failure topics after all retries are exhausted.
//   - Thread Safe: Safely snapshots event state to allow concurrent modification of the original event by the caller.
type DefaultPublisher struct {
	router      Router
	brokers     map[string]Broker
	retryConfig *RetryConfig
	taskChan    chan publishTask
	wg          sync.WaitGroup
	cancel      context.CancelFunc
	closeOnce   sync.Once
}

// NewPublisher creates and initializes a DefaultPublisher with the specified router, brokers, and config.
// The publisher starts its worker pool immediately upon creation.
//
// Example:
//
//	config := &event.PublisherConfig{
//	    Workers: 10,
//	    BufferSize: 1000,
//	    RetryConfig: &event.RetryConfig{
//	        InitialInterval: 100 * time.Millisecond,
//	        MaxElapsedTime:  30 * time.Second,
//	    },
//	}
//	pub := event.NewPublisher(router, brokers, config)
//	defer pub.Close()
func NewPublisher(router Router, brokers map[string]Broker, cfg *PublisherConfig) *DefaultPublisher {
	if cfg == nil {
		cfg = &PublisherConfig{}
	}
	if cfg.RetryConfig == nil {
		cfg.RetryConfig = &RetryConfig{
			InitialInterval: 500 * time.Millisecond,
			MaxInterval:     5 * time.Second,
			MaxElapsedTime:  15 * time.Second,
		}
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 100
	}

	ctx, cancel := context.WithCancel(context.Background())
	p := &DefaultPublisher{
		router:      router,
		brokers:     brokers,
		retryConfig: cfg.RetryConfig,
		taskChan:    make(chan publishTask, cfg.BufferSize),
		cancel:      cancel,
	}

	for i := 0; i < cfg.Workers; i++ {
		p.wg.Add(1)
		go p.worker(ctx)
	}

	return p
}

// Close gracefully shuts down the publisher.
// It stops accepting new events, waits for all currently buffered events to be processed/retried,
// and ensures background workers exit cleanly.
func (p *DefaultPublisher) Close() error {
	p.closeOnce.Do(func() {
		close(p.taskChan)
		p.wg.Wait()
		p.cancel()
	})
	return nil
}

// Publish enqueues the event for asynchronous delivery.
//
// It performs synchronous validation of the schema/routing and ensures the required broker is available.
// If valid, the event is snapshotted (struct and metadata copied) and placed in an internal buffer.
//
// Returns an error if routing fails, the broker is missing, or the internal buffer is full and the context is cancelled.
func (p *DefaultPublisher) Publish(ctx context.Context, evt *Event) error {
	if evt == nil {
		return fmt.Errorf("event cannot be nil")
	}

	config, err := p.router.RouteFor(evt.Schema, evt.EventType)
	if err != nil {
		return fmt.Errorf("routing failed: %w", err)
	}

	b, ok := p.brokers[config.QueueType]
	if !ok {
		return fmt.Errorf("no broker configured for queue type: %s", config.QueueType)
	}

	// Create a shallow copy of the struct to snapshot fields
	evtSnapshot := *evt

	// Deep clone Metadata map to avoid races with asynchronous mutation
	newMetadata := make(map[string]any, len(evt.Metadata)+1)
	for k, v := range evt.Metadata {
		newMetadata[k] = v
	}
	newMetadata["eventId"] = evt.EventId
	evtSnapshot.Metadata = newMetadata

	task := publishTask{
		evt:    &evtSnapshot,
		config: config,
		broker: b,
	}

	select {
	case p.taskChan <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *DefaultPublisher) worker(ctx context.Context) {
	defer p.wg.Done()
	for task := range p.taskChan {
		for _, topic := range task.config.Destinations {
			err := p.publishWithRetry(ctx, task.broker, topic, task.evt)
			if err != nil {
				dlqEvt := *task.evt
				dlqEvt.EventType = task.evt.EventType + task.config.GetDLQEventTypePostfix()
				dlqEvt.EventTime = time.Now().UTC()
				dlqEvt.Data = task.evt // Keep original event as data

				if dlqEvt.Metadata == nil {
					dlqEvt.Metadata = make(map[string]any)
				}
				dlqEvt.Metadata["fail_reason"] = err.Error()
				dlqEvt.Metadata["original_destination"] = topic

				dlqTopic := topic + task.config.GetDLQPostfix()
				_ = task.broker.Publish(ctx, dlqTopic, &dlqEvt)
			}
		}
	}
}

func (p *DefaultPublisher) publishWithRetry(ctx context.Context, b Broker, topic string, evt *Event) error {
	operation := func() (struct{}, error) {
		err := b.Publish(context.Background(), topic, evt) // Use background to avoid cancellation during retry
		return struct{}{}, err
	}

	eb := backoff.NewExponentialBackOff()
	if p.retryConfig != nil {
		eb.InitialInterval = p.retryConfig.InitialInterval
		eb.MaxInterval = p.retryConfig.MaxInterval
	}

	options := []backoff.RetryOption{
		backoff.WithBackOff(eb),
	}
	if p.retryConfig != nil {
		options = append(options, backoff.WithMaxElapsedTime(p.retryConfig.MaxElapsedTime))
	}

	_, err := backoff.Retry(ctx, operation, options...)
	return err
}
