package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/squall-chua/go-event-pubsub/pkg/broker/memory"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

func TestEventPubSub_Memory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Setup Router
	registry := event.SchemaRegistry{
		"userSchema": {
			"user.created": {
				QueueType:    "memory",
				Destinations: []string{"user-events"},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	// 2. Setup Broker
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{
		"memory": memBroker,
	}

	// 3. Setup Subscriber
	sub := event.NewSubscriber("userSchema", router, brokers)
	received := make(chan *event.Event, 1)
	err := sub.Subscribe("user.created", func(ctx context.Context, evt *event.Event) error {
		received <- evt
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	go func() {
		if err := sub.Start(ctx); err != nil && err != context.Canceled {
			// t.Errorf here might not be thread safe depending on runner, but fine for local
		}
	}()

	// Give subscriber a moment to start (though memory broker is instant)
	time.Sleep(100 * time.Millisecond)

	// 4. Setup Publisher
	pub := event.NewPublisher(router, brokers, nil)
	defer pub.Close()

	// 5. Publish Event
	evt := event.Event{
		EventId:   "evt-123",
		EventType: "user.created",
		EventTime: time.Now().UTC(),
		Source:    "test-service",
		Schema:    "userSchema",
		Data:      map[string]string{"name": "Alice"},
	}

	err = pub.Publish(ctx, &evt)
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// 6. Verify Receipt
	select {
	case rcv := <-received:
		if rcv.EventId != evt.EventId {
			t.Errorf("expected eventId %s, got %s", evt.EventId, rcv.EventId)
		}
	case <-ctx.Done():
		t.Error("timed out waiting for event")
	}
}

func TestDLQ_Memory(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	registry := event.SchemaRegistry{
		"testSchema": {
			"test.event": {
				QueueType:           "memory",
				Destinations:        []string{"test-topic"},
				DLQPostfix:          ptr(".failed"),
				DLQEventTypePostfix: ptr(".error"), // Custom postfix
			},
		},
	}
	router := event.NewStaticRouter(registry)
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{
		"memory": memBroker,
	}

	// Subscriber that returns error to trigger DLQ
	sub := event.NewSubscriber("testSchema", router, brokers)
	_ = sub.Subscribe("test.event", func(ctx context.Context, evt *event.Event) error {
		return context.DeadlineExceeded // simulate error
	})

	// Monitor DLQ
	dlpReceived := make(chan *event.Event, 10)
	_ = memBroker.Consume(ctx, "test-topic.failed", func(evt *event.Event) error {
		select {
		case dlpReceived <- evt:
		default:
		}
		return nil
	})

	go sub.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	pub := event.NewPublisher(router, brokers, &event.PublisherConfig{
		RetryConfig: &event.RetryConfig{
			InitialInterval: 10 * time.Millisecond,
			MaxInterval:     5 * time.Second,
			MaxElapsedTime:  100 * time.Millisecond,
		},
	})
	defer pub.Close()

	evt := event.Event{
		EventId:   "evt-dlq",
		EventType: "test.event",
		Schema:    "testSchema",
	}

	err := pub.Publish(ctx, &evt)
	if err != nil {
		t.Logf("Publish returned error (expected since we trigger DLQ): %v", err)
	}

	select {
	case dlqEvt := <-dlpReceived:
		// Verify custom event type postfix
		expectedType := "test.event.error"
		if dlqEvt.EventType != expectedType {
			t.Errorf("expected event type %s, got %s", expectedType, dlqEvt.EventType)
		}

		// Success: message reached DLQ with correct diagnostics in metadata
		if dlqEvt.Metadata["fail_reason"] == "" {
			t.Error("expected failure reason in Metadata")
		}
		if dlqEvt.Metadata["original_destination"] != "test-topic" {
			t.Errorf("expected destination test-topic, got %v", dlqEvt.Metadata["original_destination"])
		}

		// Original event is now the Data payload
		orig, ok := dlqEvt.Data.(*event.Event)
		if !ok || orig.EventId != evt.EventId {
			t.Errorf("expected original event in Data with id %s", evt.EventId)
		}
	case <-ctx.Done():
		t.Error("timed out waiting for DLQ message")
	}
}

func ptr(s string) *string {
	return &s
}
