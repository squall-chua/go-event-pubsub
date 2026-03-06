package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

// failingBroker siempre falla al publicar al DLQ
type failingBroker struct{}

func (b *failingBroker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	// Simulate "DLQ partition full" or "Broker unreachable"
	return fmt.Errorf("broker unreachable during DLQ publish")
}

func (b *failingBroker) Consume(ctx context.Context, topic string, handler func(*event.Event) error) error {
	fmt.Printf("[System] Starting consumer on topic: %s\n", topic)
	// Simulate receiving a message immediately
	go func() {
		time.Sleep(500 * time.Millisecond)
		evt := &event.Event{
			EventId:   uuid.NewString(),
			EventType: "user.login",
			Schema:    "auth_domain",
			Data:      "some-login-data",
		}
		_ = handler(evt)
	}()
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Setup Router
	registry := event.SchemaRegistry{
		"auth_domain": {
			"user.login": {
				QueueType:    "memory",
				Destinations: []string{"login-topic"},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	// 2. Setup a broker that fails DLQ publish
	brokers := map[string]event.Broker{
		"memory": &failingBroker{},
	}

	// 3. Define a Fallback Handler for the Subscriber
	fallbackCalled := make(chan struct{})
	fallbackHandler := func(ctx context.Context, evt *event.Event, dlqErr error) {
		fmt.Printf("\n[SUBSCRIBER FALLBACK TRIGGERED!]\n")
		fmt.Printf("Event ID: %s | Type: %s\n", evt.EventId, evt.EventType)
		fmt.Printf("Error writing to DLQ: %v\n", dlqErr)
		fmt.Println("ACTION: Sending alert to PagerDuty or logging to local disk...")
		close(fallbackCalled)
	}

	// 4. Setup Subscriber with a handler that always fails
	sub := event.NewSubscriber("auth_domain", router, brokers, &event.SubscriberConfig{
		DLQFallbackHandler: fallbackHandler,
	})

	_ = sub.Subscribe("user.login", func(ctx context.Context, evt *event.Event) error {
		fmt.Printf("[Consumer] Received %s, but processing failed!\n", evt.EventId)
		return errors.New("database is down") // Trigger DLQ
	})

	// 5. Start Subscriber
	fmt.Println("[System] Starting subscriber...")
	go func() {
		if err := sub.Start(ctx); err != nil && err != context.Canceled {
			log.Printf("Subscriber error: %v", err)
		}
	}()

	// Wait for fallback to be triggered
	select {
	case <-fallbackCalled:
		fmt.Println("\nSuccess: Subscriber fallback safely handled the unreachable DLQ.")
	case <-ctx.Done():
		log.Fatal("Timeout: Subscriber fallback was never called.")
	}
}
