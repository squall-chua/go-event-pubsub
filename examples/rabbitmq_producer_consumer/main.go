package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/broker/rabbitmq"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Setup the RabbitMQ Broker
	// Standard connection URL: amqp://user:pass@host:port/
	rbBroker, err := rabbitmq.NewBroker("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ (ensure it is running): %v", err)
	}
	defer rbBroker.Close()

	// 2. Setup the Router with a wildcard pattern
	registry := event.SchemaRegistry{
		"task_domain": {
			QueueType:    "rabbitmq",
			Destinations: []string{"tasks-queue"},
			DLQPostfix:   ".failed",
			Events: map[string]event.TopicConfig{
				"task.*": { // Inherits from schema defaults
				},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	brokers := map[string]event.Broker{
		"rabbitmq": rbBroker,
	}

	// 3. Setup the Subscriber with wildcard
	sub := event.NewSubscriber(router, brokers, nil)
	sub.Subscribe("task_domain", "task.*", func(ctx context.Context, evt *event.Event) error {
		fmt.Printf("[Consumer] Received %s Event: %v\n", evt.EventType, evt.Data)
		return nil
	})

	fmt.Println("[System] Starting RabbitMQ Consumer...")
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
	pub := event.NewPublisher(router, brokers, &event.PublisherConfig{
		Workers: 5,
		RetryConfig: &event.RetryConfig{
			InitialInterval: 500 * time.Millisecond,
			MaxElapsedTime:  5 * time.Second,
		},
	})
	defer pub.Close()

	// 5. Publish a specific event that matches the wildcard
	evt := &event.Event{
		EventId:    uuid.NewString(),
		EventType:  "task.created", // Matches task.*
		EventTime:  time.Now().UTC(),
		User:       "user_admin",
		Source:     "task-service",
		Schema:     "task_domain",
		ResourceID: "task_99",
		Data:       map[string]any{"task_name": "Write Documentation", "priority": "high"},
	}

	fmt.Printf("[Publisher] Publishing task %s to RabbitMQ...\n", evt.ResourceID)
	if err := pub.Publish(ctx, evt); err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
	fmt.Println("RabbitMQ example finished.")
}
