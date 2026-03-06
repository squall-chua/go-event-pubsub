package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

// LoggerBroker is a custom implementation of the event.Broker interface.
// Applications can implement this interface to support any messaging system (e.g., Redis, AWS SNS/SQS, NATS).
type LoggerBroker struct {
	// Add your configuration here (e.g., connection pool, API keys)
}

// NewLoggerBroker creates a new custom broker instance.
func NewLoggerBroker() *LoggerBroker {
	return &LoggerBroker{}
}

// Publish implements the low-level delivery logic to your messaging system.
func (b *LoggerBroker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	fmt.Printf("[CustomBroker] PUBLISH to topic: %s | EventID: %s\n", topic, evt.EventId)
	// Here you would marshal the event to JSON and send it over the wire.
	return nil
}

// Consume registers a handler for your messaging system's consumption loop.
func (b *LoggerBroker) Consume(ctx context.Context, topic string, handler func(*event.Event) error) error {
	fmt.Printf("[CustomBroker] REGISTER consumer for topic: %s\n", topic)

	// Note: In a real implementation, you would start a background goroutine
	// that polls or receives messages from your broker and calls the handler.
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Setup Router and your Custom Broker
	registry := event.SchemaRegistry{
		"infra_domain": {
			"log.message": {
				QueueType:    "custom-logger",
				Destinations: []string{"audit-log-stream"},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	// 2. Register the custom broker in the map
	customBroker := NewLoggerBroker()
	brokers := map[string]event.Broker{
		"custom-logger": customBroker,
	}

	// 3. Setup Publisher using the custom broker
	pub := event.NewPublisher(router, brokers, nil)
	defer pub.Close()

	// 4. Publish an Event
	evt := &event.Event{
		EventId:   uuid.NewString(),
		EventType: "log.message",
		Schema:    "infra_domain",
		Data:      "this is handled by a custom broker implementation",
	}

	fmt.Println("[System] Publishing event through custom broker...")
	if err := pub.Publish(ctx, evt); err != nil {
		log.Fatal(err)
	}

	// Wait for background worker to catch up
	time.Sleep(100 * time.Millisecond)
	fmt.Println("Custom Broker example finished.")
}
