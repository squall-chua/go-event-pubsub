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
	kBroker := kafka.NewBroker(kafka.Config{
		Brokers: brokersList,
		Writer: kafka.WriterConfig{
			BatchSize:    100,
			WriteTimeout: 5 * time.Second,
		},
		Reader: kafka.ReaderConfig{
			GroupID: "example-group",
		},
	})

	// 2. Setup the Router
	registry := event.SchemaRegistry{
		"order_domain": {
			"order.created": {
				QueueType:    "kafka",
				Destinations: []string{"orders-topic"},
				DLQPostfix:   event.Ptr(".failed"), // Automatic DLQ management
			},
		},
	}
	router := event.NewStaticRouter(registry)

	brokers := map[string]event.Broker{
		"kafka": kBroker,
	}

	// 3. Setup the Subscriber
	sub := event.NewSubscriber("order_domain", router, brokers, nil)
	sub.Subscribe("order.created", func(ctx context.Context, evt *event.Event) error {
		fmt.Printf("[Consumer] Processing Order: %v\n", evt.Data)
		return nil
	})

	go func() {
		fmt.Println("[System] Starting Kafka Consumer...")
		if err := sub.Start(ctx); err != nil && err != context.Canceled {
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

	// 5. Publish
	evt := &event.Event{
		EventId:    uuid.NewString(),
		EventType:  "order.created",
		EventTime:  time.Now().UTC(),
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
