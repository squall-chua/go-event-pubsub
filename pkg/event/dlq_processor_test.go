package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/squall-chua/go-event-pubsub/pkg/broker/memory"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDLQProcessor_Process(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Setup our mock router and Memory broker
	registry := event.SchemaRegistry{
		"testSchema": {
			"test.event": {
				QueueType:    "memory",
				Destinations: []string{"main-topic"},
				DLQPostfix:   event.Ptr(".dead"),
			},
		},
	}
	router := event.NewStaticRouter(registry)
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{"memory": memBroker}

	// 2. Create the Publisher internally used by the Processor to republish
	pub := event.NewPublisher(router, brokers, &event.PublisherConfig{Workers: 1})
	defer pub.Close()

	// 3. Create the DLQProcessor
	processor := event.NewDLQProcessor(memBroker, pub)

	// 4. Create a mock failed event that is sitting in the DLQ
	originalEvt := &event.Event{
		EventId:   "evt-123",
		EventType: "test.event",
		Schema:    "testSchema",
		Metadata: map[string]any{
			"custom_tracking": "track-123",
		},
	}

	dlqEvt := &event.Event{
		EventType: "test.event.failed",
		EventTime: time.Now().UTC(),
		Data:      originalEvt, // Original event is wrapped
		Metadata: map[string]any{
			"fail_reason":          "simulated error",
			"original_destination": "main-topic",
		},
	}

	// 5. Setup a standalone consumer on the MAIN topic to verify successful recovery
	dlqTopic := "main-topic.dead"
	recoveredEvents := make(chan *event.Event, 1)
	err := memBroker.Consume(ctx, "main-topic", func(evt *event.Event) error {
		recoveredEvents <- evt
		return nil
	})
	require.NoError(t, err)

	// 7. Run the Processor (in background)
	go func() {
		// Only recover if the failure reason was "simulated error"
		err := processor.Process(ctx, dlqTopic, func(evt *event.Event) bool {
			reason, ok := evt.Metadata["fail_reason"].(string)
			return ok && reason == "simulated error"
		})
		if err != nil && err != context.Canceled {
			t.Errorf("Processor error: %v", err)
		}
	}()

	// 8. Inject the event into the DLQ topic manually AFTER starting processor
	// Note: Memory broker requires subscribers to be present before publish
	time.Sleep(50 * time.Millisecond) // ensure processor subscription is registered
	err = memBroker.Publish(ctx, dlqTopic, dlqEvt)
	require.NoError(t, err)

	// 9. Assert Recovery
	select {
	case evt := <-recoveredEvents:
		assert.Equal(t, "evt-123", evt.EventId, "Event ID should remain exactly the same")
		assert.Equal(t, "test.event", evt.EventType)

		// The original custom metadata should persist
		assert.Equal(t, "track-123", evt.Metadata["custom_tracking"])

		// The failure artifacts MUST be stripped to prevent corruption
		_, hasReason := evt.Metadata["fail_reason"]
		assert.False(t, hasReason, "fail_reason should have been stripped from the recovered event")

		_, hasDest := evt.Metadata["original_destination"]
		assert.False(t, hasDest, "original_destination should have been stripped from the recovered event")

	case <-ctx.Done():
		t.Fatal("Timeout waiting for event to be recovered and republished")
	}
}
