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

	// 2. Setup the Router
	// We'll map "task.created" events to a topic named "tasks-queue".
	registry := event.SchemaRegistry{
		"task_domain": {
			"task.created": {
				QueueType:    "rabbitmq",
				Destinations: []string{"tasks-queue"},
				DLQPostfix:   event.Ptr(".failed"), // Auto-declare tasks-queue.failed
			},
		},
	}
	router := event.NewStaticRouter(registry)

	brokers := map[string]event.Broker{
		"rabbitmq": rbBroker,
	}

	// 3. Setup the Subscriber
	sub := event.NewSubscriber("task_domain", router, brokers, nil)
	sub.Subscribe("task.created", func(ctx context.Context, evt *event.Event) error {
		fmt.Printf("[Consumer] Received Task: %v\n", evt.Data)
		return nil
	})

	go func() {
		fmt.Println("[System] Starting RabbitMQ Consumer...")
		if err := sub.Start(ctx); err != nil && err != context.Canceled {
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

	// 5. Publish
	evt := &event.Event{
		EventId:    uuid.NewString(),
		EventType:  "task.created",
		EventTime:  time.Now().UTC(),
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
