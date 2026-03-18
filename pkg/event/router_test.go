package event_test

import (
	"testing"

	"github.com/squall-chua/go-event-pubsub/pkg/event"
	"github.com/stretchr/testify/assert"
)

func TestRouter_Inheritance(t *testing.T) {
	registry := event.SchemaRegistry{
		"domainA": {
			QueueType:  "brokerA",
			DLQPostfix: ".failedA",
			Events: map[string]event.TopicConfig{
				"event.withOverrides": {
					QueueType:    "brokerOverride",
					Destinations: []string{"topic1"},
					DLQPostfix:   event.Ptr(".failedOverride"),
				},
				"event.withDefaults": {
					Destinations: []string{"topic2"},
				},
			},
		},
		"domainB": {
			QueueType: "brokerB",
			Events: map[string]event.TopicConfig{
				"event.noDLQ": {
					Destinations: []string{"topic3"},
				},
			},
		},
	}
	router := event.NewStaticRouter(registry)

	tests := []struct {
		name              string
		schema            string
		eventType         string
		expectedQueue     string
		expectedDLQFix    string
	}{
		{
			name:              "full override",
			schema:            "domainA",
			eventType:         "event.withOverrides",
			expectedQueue:     "brokerOverride",
			expectedDLQFix:    ".failedOverride",
		},
		{
			name:              "use schema defaults",
			schema:            "domainA",
			eventType:         "event.withDefaults",
			expectedQueue:     "brokerA",
			expectedDLQFix:    ".failedA",
		},
		{
			name:              "no DLQ fallback if not in schema",
			schema:            "domainB",
			eventType:         "event.noDLQ",
			expectedQueue:     "brokerB",
			expectedDLQFix:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := router.RouteFor(tt.schema, tt.eventType)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedQueue, cfg.QueueType)
			assert.Equal(t, tt.expectedDLQFix, cfg.GetDLQPostfix())
		})
	}
}
