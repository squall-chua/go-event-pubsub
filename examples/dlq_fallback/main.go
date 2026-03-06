package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

// failingBroker is a helper to simulate a completely broken infrastructure.
type failingBroker struct{}

func (b *failingBroker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	// Simulate "Connection refused" or "Broker unreachable"
	return fmt.Errorf("dial tcp 127.0.0.1:9092: connect: connection refused")
}

func (b *failingBroker) Consume(ctx context.Context, topic string, handler func(*event.Event) error) error {
	return nil
}

func (b *failingBroker) Close() error {
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Setup Router points to a "kafka" broker that will fail.
	registry := event.SchemaRegistry{
		"critical_domain": {
			"data.sync": {
				QueueType:    "kafka",
				Destinations: []string{"sync-topic"},
				DLQPostfix:   event.Ptr(".dead"),
			},
		},
	}
	router := event.NewStaticRouter(registry)

	// 2. Register our failing broker.
	brokers := map[string]event.Broker{
		"kafka": &failingBroker{},
	}

	// 3. Define a Fallback Handler.
	// This is called when the event cannot even reach the DLQ.
	fallbackCalled := make(chan struct{})
	fallbackHandler := func(ctx context.Context, evt *event.Event, dlqErr error) {
		fmt.Printf("\n[FALLBACK HANDLER TRIGGERED!]\n")
		fmt.Printf("Event ID: %s\n", evt.EventId)
		fmt.Printf("Failed to reach DLQ because: %v\n", dlqErr)
		fmt.Println("ACTION: Persisting event to local emergency log file...")

		// In a real app, you'd write to local disk, S3, or an emergency DB here.
		close(fallbackCalled)
	}

	// 4. Setup Publisher with aggressive retries for the example.
	pub := event.NewPublisher(router, brokers, &event.PublisherConfig{
		Workers: 1,
		RetryConfig: &event.RetryConfig{
			InitialInterval: 10 * time.Millisecond,
			MaxElapsedTime:  50 * time.Millisecond, // Ensure it exhausts retries quickly
		},
		DLQFallbackHandler: fallbackHandler,
	})
	defer pub.Close()

	// 5. Publish an event.
	evt := &event.Event{
		EventId:   uuid.NewString(),
		EventType: "data.sync",
		Schema:    "critical_domain",
		Data:      "highly important data",
	}

	fmt.Printf("[Publisher] Attempting to send critical event %s...\n", evt.EventId)
	fmt.Println("[System] Note: Expect logs about retries and DLQ failures below.")

	if err := pub.Publish(ctx, evt); err != nil {
		log.Fatal(err)
	}

	// Wait for the background worker to exhaust retries and hit the fallback.
	select {
	case <-fallbackCalled:
		fmt.Println("\nSuccess: Fallback logic safely handled the unreachable broker event.")
	case <-ctx.Done():
		log.Fatal("Timeout: Fallback handler was never called.")
	}
}
