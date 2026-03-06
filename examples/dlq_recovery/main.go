package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/squall-chua/go-event-pubsub/pkg/broker/memory"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// 1. Setup Router with DLQ
	registry := event.SchemaRegistry{
		"maintenance_domain": {
			"system.update": {
				QueueType:    "memory",
				Destinations: []string{"update-topic"},
				DLQPostfix:   event.Ptr(".dead"),
			},
		},
	}
	router := event.NewStaticRouter(registry)
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{"memory": memBroker}

	// 2. Setup Publisher and DLQProcessor
	pub := event.NewPublisher(router, brokers, nil)
	defer pub.Close()
	processor := event.NewDLQProcessor(memBroker, pub)

	// 3. Setup a consumer that simulates a transient error for specific events
	errCount := 0
	sub := event.NewSubscriber("maintenance_domain", router, brokers, nil)
	sub.Subscribe("system.update", func(ctx context.Context, evt *event.Event) error {
		if errCount < 1 {
			errCount++
			fmt.Printf("[Consumer] Simulating transient error for event %s\n", evt.EventId)
			return fmt.Errorf("transient database failure")
		}
		fmt.Printf("[Consumer] Successfully processed event %s after recovery!\n", evt.EventId)
		return nil
	})
	go sub.Start(ctx)

	// 4. Setup a simple monitor to watch the DLQ
	dlqTopic := "update-topic.dead"
	fmt.Println("[System] Monitoring DLQ for recovery opportunities...")

	// 5. Publish an event that will fail and go to DLQ
	evtID := uuid.NewString()
	fmt.Printf("[Publisher] Sending event %s\n", evtID)
	_ = pub.Publish(ctx, &event.Event{
		EventId:   evtID,
		EventType: "system.update",
		Schema:    "maintenance_domain",
		Data:      "some update payload",
	})

	// Give it some time to fail and land in DLQ
	time.Sleep(500 * time.Millisecond)

	// 6. Run DLQ Recovery
	// In a real system, this could be a CLI command or a cron job.
	fmt.Println("[Processor] Running recovery for 'transient' failure reasons...")

	// Recover only if reason contains 'transient'
	err := processor.Process(ctx, dlqTopic, func(evt *event.Event) bool {
		reason, _ := evt.Metadata["fail_reason"].(string)
		return strings.Contains(reason, "transient")
	})
	if err != nil && err != context.Canceled {
		log.Printf("Recovery error: %v", err)
	}

	// Wait for the recovered event to be processed
	time.Sleep(1 * time.Second)
	fmt.Println("DLQ Recovery example finished.")
}
