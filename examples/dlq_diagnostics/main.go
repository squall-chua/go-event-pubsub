package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/broker/memory"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Setup Router with DLQ configuration
	registry := event.SchemaRegistry{
		"payment_domain": {
			"payment.processed": {
				QueueType:           "memory",
				Destinations:        []string{"payments-topic"},
				DLQPostfix:          event.Ptr(".dead"),   // Physical topic: payments-topic.dead
				DLQEventTypePostfix: event.Ptr(".failed"), // Event type: payment.processed.failed
			},
		},
	}
	router := event.NewStaticRouter(registry)
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{"memory": memBroker}

	// 2. Setup Subscriber with a failing handler
	sub := event.NewSubscriber("payment_domain", router, brokers, nil)
	sub.Subscribe("payment.processed", func(ctx context.Context, evt *event.Event) error {
		fmt.Println("[Consumer] Received payment. Processing...")
		return errors.New("database connection lost") // Trigger DLQ
	})

	// 3. Monitor the DLQ Topic
	_ = memBroker.Consume(ctx, "payments-topic.dead", func(evt *event.Event) error {
		fmt.Printf("[DLQ Monitor] Caught failed event!\n")
		fmt.Printf("  - Type: %s\n", evt.EventType)
		fmt.Printf("  - Reason: %v\n", evt.Metadata["fail_reason"])
		fmt.Printf("  - Original Destination: %v\n", evt.Metadata["original_destination"])

		// The original event is in the Data field
		origEvt := evt.Data.(*event.Event)
		fmt.Printf("  - Original Event ID: %s\n", origEvt.EventId)
		return nil
	})

	go sub.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// 4. Publish Event
	pub := event.NewPublisher(router, brokers, nil)
	defer pub.Close()

	evt := &event.Event{
		EventId:   uuid.NewString(),
		EventType: "payment.processed",
		Schema:    "payment_domain",
		Data:      map[string]any{"amount": 50.0},
	}

	fmt.Println("[Publisher] Sending payment event...")
	_ = pub.Publish(ctx, evt)

	<-ctx.Done()
}
