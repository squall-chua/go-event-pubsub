package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/broker/memory"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Setup the Router
	// The router maps logical events to physical destinations.
	registry := event.SchemaRegistry{
		"user_domain": {
			"user.registered": {
				QueueType:    "memory",
				Destinations: []string{"user-signup-topic"},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	// 2. Setup the Broker
	// We use the in-memory broker for this basic example.
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{
		"memory": memBroker,
	}

	// 3. Setup the Subscriber
	sub := event.NewSubscriber("user_domain", router, brokers, nil)

	// Register a handler for the "user.registered" event
	sub.Subscribe("user.registered", func(ctx context.Context, evt *event.Event) error {
		fmt.Printf("[Consumer] Received event: %s, ID: %s, Data: %v\n",
			evt.EventType, evt.EventId, evt.Data)
		return nil
	})

	// Start the subscriber in the background
	go func() {
		if err := sub.Start(ctx); err != nil && err != context.Canceled {
			log.Fatalf("Subscriber error: %v", err)
		}
	}()

	// Give the subscriber a bit to start
	time.Sleep(100 * time.Millisecond)

	// 4. Setup the Publisher
	pub := event.NewPublisher(router, brokers, &event.PublisherConfig{
		Workers: 5,
	})
	defer pub.Close()

	// 5. Publish an Event
	evt := &event.Event{
		EventId:   uuid.NewString(),
		EventType: "user.registered",
		EventTime: time.Now().UTC(),
		Source:    "auth-service",
		Schema:    "user_domain",
		Data:      map[string]string{"email": "test@example.com"},
	}

	fmt.Printf("[Publisher] Sending event: %s\n", evt.EventId)
	if err := pub.Publish(ctx, evt); err != nil {
		log.Fatal(err)
	}

	// Wait for processing to complete
	<-ctx.Done()
	fmt.Println("Example finished.")
}
