package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/broker/kafka"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	brokersList := []string{"localhost:9092"}

	// 1. Setup the Kafka Broker
	kBroker, err := kafka.NewBroker(kafka.Config{
		Brokers: brokersList,
		Writer: kafka.WriterConfig{
			BatchSize:    100,
			WriteTimeout: 5 * time.Second,
		},
		Reader: kafka.ReaderConfig{
			GroupID: "example-group",
		},
	})
	if err != nil {
		log.Fatalf("Failed to create Kafka broker: %v", err)
	}

	// 2. Setup the Router with a wildcard
	registry := event.SchemaRegistry{
		"order_domain": {
			QueueType:    "kafka",
			Destinations: []string{"orders-topic"},
			DLQPostfix:   ".failed",
			Events: map[string]event.TopicConfig{
				"order.*": { // Inherits Kafka broker, destinations and DLQ from schema
				},
				"order.internal": {
					QueueType:    "memory", // Local override
					Destinations: []string{"internal-orders"},
				},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	brokers := map[string]event.Broker{
		"kafka": kBroker,
	}

	// 3. Setup the Subscriber with wildcard
	sub := event.NewSubscriber(router, brokers, nil)

	// Subscribing to order.* will receive order.created
	sub.Subscribe("order_domain", "order.*", func(ctx context.Context, evt *event.Event) error {
		fmt.Printf("[Consumer] Processing %s Event: %v\n", evt.EventType, evt.Data)
		return nil
	})

	fmt.Println("[System] Starting Kafka Consumer...")
	errCh, err := sub.Start(ctx)
	if err != nil {
		log.Fatalf("Failed to start subscriber: %v", err)
	}
	go func() {
		for err := range errCh {
			log.Printf("Subscriber error (ensure Kafka is running): %v", err)
		}
	}()

	// 4. Setup the Publisher
	pub := event.NewPublisher(router, brokers, &event.PublisherConfig{
		Workers: 10,
		RetryConfig: &event.RetryConfig{
			InitialInterval: 500 * time.Millisecond,
			MaxElapsedTime:  5 * time.Second,
		},
	})
	defer pub.Close()

	// 5. Publish a specific event that matches the wildcard
	evt := &event.Event{
		EventId:    uuid.NewString(),
		EventType:  "order.created", // Matches order.*
		EventTime:  time.Now().UTC(),
		User:       "user_99",
		Source:     "checkout-service",
		Schema:     "order_domain",
		ResourceID: "order_66",
		Data:       map[string]any{"amount": 100.50, "currency": "USD"},
	}

	fmt.Printf("[Publisher] Publishing order %s to Kafka...\n", evt.ResourceID)
	if err := pub.Publish(ctx, evt); err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
	fmt.Println("Kafka example finished.")
}
