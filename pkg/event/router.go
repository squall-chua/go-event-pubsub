package event

import (
	"fmt"
	"strings"
	"sync"
)

// TopicConfig defines the routing and delivery behavior for a specific event type.
type TopicConfig struct {
	// QueueType identifies the broker to use (e.g., "kafka", "rabbitmq", "memory").
	// If empty, will use the default from the parent EventSchema.
	QueueType string `yaml:"queue_type,omitempty" json:"queueType,omitempty"`
	// Destinations is a list of physical topics or queues where the event will be sent.
	Destinations []string `yaml:"destinations" json:"destinations"`
	// DLQPostfix is an optional postfix appended to each destination for failure routing.
	// If nil, and no default is provided in EventSchema, DLQ routing is disabled.
	DLQPostfix *string `yaml:"dlq_postfix,omitempty" json:"dlqPostfix,omitempty"`
	// DLQEventTypePostfix is an optional postfix appended to the EventType when moved to DLQ. Defaults to ".failed".
	DLQEventTypePostfix *string `yaml:"dlq_event_type_postfix,omitempty" json:"dlqEventTypePostfix,omitempty"`
}

// GetDLQPostfix returns the configured topic postfix.
func (t *TopicConfig) GetDLQPostfix() string {
	if t.DLQPostfix == nil {
		return ""
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

// EventSchema defines routing rules and defaults for multiple event types within a single schema or domain.
type EventSchema struct {
	// QueueType is the default broker for all events in this schema.
	QueueType string `yaml:"queue_type" json:"queueType"`
	// DLQPostfix is the default DLQ postfix for all events in this schema.
	DLQPostfix string `yaml:"dlq_postfix" json:"dlqPostfix"`
	// DLQEventTypePostfix is the default DLQ event type postfix for all events in this schema.
	DLQEventTypePostfix string `yaml:"dlq_event_type_postfix" json:"dlqEventTypePostfix"`
	// Destinations is the default list of targets for all events in this schema.
	Destinations []string `yaml:"destinations" json:"destinations"`
	// Events maps event types to their specific routing configurations.
	Events map[string]TopicConfig `yaml:"events" json:"events"`
}

// SchemaRegistry is the top-level collection of all event schemas in the system.
type SchemaRegistry map[string]EventSchema

// Router defines how a logical event maps to its physical destinations and delivery rules.
type Router interface {
	// RouteFor returns the delivery configuration for a specific schema and event type.
	RouteFor(schema, eventType string) (*TopicConfig, error)
}

// StaticRouter is a simple, thread-safe implementation of the Router interface using an in-memory registry.
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

// RouteFor looks up the routing configuration and applies defaults from the schema.
func (r *StaticRouter) RouteFor(schema, eventType string) (*TopicConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	s, ok := r.registry[schema]
	if !ok {
		return nil, fmt.Errorf("schema %s not found", schema)
	}

	// 1. Try exact match
	cfg, ok := s.Events[eventType]
	if !ok {
		// 2. Try wildcard matches
		// Note: we take the first matching wildcard.
		for pattern, wildcardCfg := range s.Events {
			if r.match(pattern, eventType) {
				cfg = wildcardCfg
				ok = true
				break
			}
		}
	}

	if !ok {
		return nil, fmt.Errorf("event type %s not found in schema %s", eventType, schema)
	}

	// Resolve overrides
	resolved := cfg
	if resolved.QueueType == "" {
		resolved.QueueType = s.QueueType
	}
	if len(resolved.Destinations) == 0 {
		resolved.Destinations = s.Destinations
	}
	if resolved.DLQPostfix == nil && s.DLQPostfix != "" {
		resolved.DLQPostfix = &s.DLQPostfix
	}
	if resolved.DLQEventTypePostfix == nil && s.DLQEventTypePostfix != "" {
		resolved.DLQEventTypePostfix = &s.DLQEventTypePostfix
	}

	return &resolved, nil
}

func (r *StaticRouter) match(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	if len(pattern) > 0 && pattern[len(pattern)-1] == '*' {
		prefix := pattern[:len(pattern)-1]
		return strings.HasPrefix(name, prefix)
	}
	return pattern == name
}

// Ptr is a helper utility to return a pointer to a string literal.
func Ptr(s string) *string {
	return &s
}
