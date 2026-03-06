package event_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/squall-chua/go-event-pubsub/pkg/broker/memory"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
	"github.com/stretchr/testify/assert"
)

// failingBroker always returns an error on Publish
type failingBroker struct {
	memory.Broker
}

func (b *failingBroker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	return fmt.Errorf("network down")
}

func TestDLQFallbackHandler(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Setup a broker that fails both main and DLQ publish
	broker := &failingBroker{}
	brokers := map[string]event.Broker{"memory": broker}

	registry := event.SchemaRegistry{
		"test": {
			"event": {
				QueueType:    "memory",
				Destinations: []string{"topic"},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	// 2. Setup a fallback handler
	var fallbackEvent *event.Event
	var fallbackErr error
	var wg sync.WaitGroup
	wg.Add(1)

	fallback := func(ctx context.Context, evt *event.Event, err error) {
		fallbackEvent = evt
		fallbackErr = err
		wg.Done()
	}

	pub := event.NewPublisher(router, brokers, &event.PublisherConfig{
		Workers: 1,
		RetryConfig: &event.RetryConfig{
			MaxElapsedTime: 10 * time.Millisecond, // Speed up test
		},
		DLQFallbackHandler: fallback,
	})
	defer pub.Close()

	// 3. Publish
	evt := &event.Event{
		EventId:   "fallback-test",
		EventType: "event",
		Schema:    "test",
	}
	_ = pub.Publish(ctx, evt)

	// 4. Wait for fallback to be called
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.Equal(t, "fallback-test", fallbackEvent.EventId)
		assert.Contains(t, fallbackErr.Error(), "network down")
	case <-ctx.Done():
		t.Fatal("fallback handler never called")
	}
}
