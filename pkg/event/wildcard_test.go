package event_test

import (
	"context"
	"testing"
	"time"

	"github.com/squall-chua/go-event-pubsub/pkg/broker/memory"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
	"github.com/stretchr/testify/require"
)

func TestSubscriber_Wildcard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	registry := event.SchemaRegistry{
		"testSchema": {
			QueueType: "memory",
			Events: map[string]event.TopicConfig{
				"test-*": {
					Destinations: []string{"test-topic"},
				},
				"other": {
					Destinations: []string{"other-topic"},
				},
			},
		},
	}
	router := event.NewStaticRouter(registry)
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{"memory": memBroker}

	sub := event.NewSubscriber(router, brokers, nil)

	received := make(chan string, 10)

	// Subscribe with wildcard
	err := sub.Subscribe("testSchema", "test-*", func(ctx context.Context, evt *event.Event) error {
		received <- evt.EventType
		return nil
	})
	require.NoError(t, err)

	_, err = sub.Start(ctx)
	require.NoError(t, err)

	// Wait a bit for subscriptions to register
	time.Sleep(50 * time.Millisecond)

	// 1. Test wildcard match: test-abc
	err = memBroker.Publish(ctx, "test-topic", &event.Event{
		EventType: "test-abc",
		Schema:    "testSchema",
	})
	require.NoError(t, err)

	select {
	case et := <-received:
		require.Equal(t, "test-abc", et)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for test-abc")
	}

	// 2. Test wildcard match: test-xyz
	err = memBroker.Publish(ctx, "test-topic", &event.Event{
		EventType: "test-xyz",
		Schema:    "testSchema",
	})
	require.NoError(t, err)

	select {
	case et := <-received:
		require.Equal(t, "test-xyz", et)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for test-xyz")
	}

	// 4. Test publisher-routed wildcard: should match test-*
	pub := event.NewPublisher(router, brokers, nil)
	defer pub.Close()
	err = pub.Publish(ctx, &event.Event{
		EventType: "test-routed",
		Schema:    "testSchema",
		Data:      "payload",
	})
	require.NoError(t, err)

	select {
	case et := <-received:
		require.Equal(t, "test-routed", et)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timeout waiting for publisher-routed event")
	}
}

func TestSubscriber_GlobalWildcard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	registry := event.SchemaRegistry{
		"testSchema": {
			QueueType: "memory",
			Events: map[string]event.TopicConfig{
				"*": {
					Destinations: []string{"all-topic"},
				},
			},
		},
	}
	router := event.NewStaticRouter(registry)
	memBroker := memory.NewBroker()
	brokers := map[string]event.Broker{"memory": memBroker}

	sub := event.NewSubscriber(router, brokers, nil)
	received := make(chan string, 10)

	err := sub.Subscribe("testSchema", "*", func(ctx context.Context, evt *event.Event) error {
		received <- evt.EventType
		return nil
	})
	require.NoError(t, err)

	_, err = sub.Start(ctx)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)

	// Any event should match
	eventTypes := []string{"foo", "bar", "test.one"}
	for _, et := range eventTypes {
		err = memBroker.Publish(ctx, "all-topic", &event.Event{
			EventType: et,
			Schema:    "testSchema",
		})
		require.NoError(t, err)

		select {
		case actual := <-received:
			require.Equal(t, et, actual)
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("Timeout waiting for %s", et)
		}
	}
}
