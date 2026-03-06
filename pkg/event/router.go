package event

import (
	"fmt"
	"sync"
)

// TopicConfig defines the routing and delivery behavior for a specific event type.
type TopicConfig struct {
	// QueueType identifies the broker to use (e.g., "kafka", "rabbitmq", "memory").
	QueueType string `yaml:"queue_type" json:"queueType"`
	// Destinations is a list of physical topics or queues where the event will be sent.
	Destinations []string `yaml:"destinations" json:"destinations"`
	// DLQPostfix is an optional postfix appended to each destination for failure routing. Defaults to ".dlq".
	DLQPostfix *string `yaml:"dlq_postfix,omitempty" json:"dlqPostfix,omitempty"`
	// DLQEventTypePostfix is an optional postfix appended to the EventType when moved to DLQ. Defaults to ".failed".
	DLQEventTypePostfix *string `yaml:"dlq_event_type_postfix,omitempty" json:"dlqEventTypePostfix,omitempty"`
}

// GetDLQPostfix returns the configured topic postfix or the default ".dlq".
func (t *TopicConfig) GetDLQPostfix() string {
	if t.DLQPostfix == nil {
		return ".dlq"
	}
	return *t.DLQPostfix
}

// GetDLQEventTypePostfix returns the configured event type postfix or the default ".failed".
func (t *TopicConfig) GetDLQEventTypePostfix() string {
	if t.DLQEventTypePostfix == nil {
		return ".failed"
	}
	return *t.DLQEventTypePostfix
}

// EventSchema defines routing rules for multiple event types within a single schema or domain.
type EventSchema map[string]TopicConfig

// SchemaRegistry is the top-level collection of all event schemas in the system.
type SchemaRegistry map[string]EventSchema

// Router defines how a logical event maps to its physical destinations and delivery rules.
type Router interface {
	// RouteFor returns the delivery configuration for a specific schema and event type.
	RouteFor(schema, eventType string) (*TopicConfig, error)
}

// StaticRouter is a simple, thread-safe implementation of the Router interface using an in-memory registry.
//
// Example:
//
//	registry := event.SchemaRegistry{
//	    "user_domain": {
//	        "user.created": {
//	            QueueType:    "kafka",
//	            Destinations: []string{"prod.users.created"},
//	            DLQPostfix:   event.Ptr(".failed_messages"),
//	        },
//	    },
//	}
//	router := event.NewStaticRouter(registry)
type StaticRouter struct {
	mu       sync.RWMutex
	registry SchemaRegistry
}

// NewStaticRouter creates and returns a new StaticRouter with the provided registry.
func NewStaticRouter(registry SchemaRegistry) *StaticRouter {
	return &StaticRouter{
		registry: registry,
	}
}

// RouteFor looks up the routing configuration for the given schema and event type in the internal registry.
func (r *StaticRouter) RouteFor(schema, eventType string) (*TopicConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, ok := r.registry[schema]
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schema)
	}

	t, ok := s[eventType]
	if !ok {
		return nil, fmt.Errorf("event type %s not found in schema %s", eventType, schema)
	}

	return &t, nil
}

// Ptr is a helper utility to return a pointer to a string literal.
func Ptr(s string) *string {
	return &s
}
