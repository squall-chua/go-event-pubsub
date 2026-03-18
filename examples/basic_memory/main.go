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
			Events: map[string]event.TopicConfig{
				"user.*": { // Handle all types under 'user.*' topic
					QueueType:    "memory",
					Destinations: []string{"user-topic"},
				},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	// 2. Broker setup
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{"memory": memBroker}

	// 3. Setup the Subscriber with wildcards
	sub := event.NewSubscriber(router, brokers, nil)

	// Subscribing to "user.*" picks up any specific event starting with "user."
	sub.Subscribe("user_domain", "user.*", func(ctx context.Context, evt *event.Event) error {
		fmt.Printf("[Consumer] Received %s event for User: %s (Data: %v)\n",
			evt.EventType, evt.User, evt.Data)
		return nil
	})

	errCh, err := sub.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start subscriber: %v", err)
	}
	go func() {
		for err := range errCh {
			log.Printf("Subscriber error: %v", err)
		}
	}()

	// 4. Setup the Publisher
	pub := event.NewPublisher(router, brokers, nil)
	defer pub.Close()

	// 5. Publish different events that match the wildcard
	eventsToSend := []string{"user.registered", "user.modified", "user.deleted"}

	for _, et := range eventsToSend {
		evt := &event.Event{
			EventId:   uuid.NewString(),
			EventType: et, // These all match the "user.*" pattern
			User:      "user_123",
			Source:    "auth-service",
			Schema:    "user_domain",
			Data:      map[string]any{"type": et},
		}

		fmt.Printf("[Publisher] Sending: %s\n", et)
		if err := pub.Publish(ctx, evt); err != nil {
			log.Fatal(err)
		}
	}

	// Wait for processing to complete
	time.Sleep(500 * time.Millisecond)
	fmt.Println("Example finished.")
}
