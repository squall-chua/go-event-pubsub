package event

import (
	"context"
	"fmt"
)

// DLQFilter allows filtering which DLQ events should be reprocessed.
// Return true to reprocess, false to skip (or drop).
type DLQFilter func(evt *Event) bool

// DLQProcessor handles recovering events from a Dead Letter Queue.
// It reads from a DLQ topic, extracts the original event, and optionally republishes it.
type DLQProcessor struct {
	broker Broker
	pub    Publisher
}

// NewDLQProcessor creates a new DLQProcessor.
// It requires a Broker to consume the DLQ topic and a Publisher to re-inject the recovered events.
func NewDLQProcessor(broker Broker, pub Publisher) *DLQProcessor {
	return &DLQProcessor{
		broker: broker,
		pub:    pub,
	}
}

// Process begins consuming messages from the specified dlqTopic.
// For every message received, it applies the optional filter.
// If the filter passes (or is nil), the original event is extracted and republished
// to its original destination via the configured Publisher.
func (p *DLQProcessor) Process(ctx context.Context, dlqTopic string, filter DLQFilter) error {
	return p.broker.Consume(ctx, dlqTopic, func(dlqEvt *Event) error {
		if filter != nil && !filter(dlqEvt) {
			return nil // Skip this event
		}

		// Extract the original event from the Data payload
		// Assuming the data is unmarshalled into a map or the struct itself
		var origEvt *Event
		switch data := dlqEvt.Data.(type) {
		case *Event:
			origEvt = data
		case Event:
			origEvt = &data
		default:
			// In a real scenario where it's parsed from JSON, data might be a map[string]any.
			// We might need an intermediate struct or json.Marshal/Unmarshal to recover it cleanly,
			// but if using our Memory broker or strict typing, this cast works.
			return fmt.Errorf("failed to cast DLQ data back to Event, got type %T", dlqEvt.Data)
		}

		// Remove the DLQ diagnostic metadata to prevent pollution if it fails again
		delete(origEvt.Metadata, "fail_reason")
		delete(origEvt.Metadata, "original_destination")

		// Republish forces the event back through the router to its defined destinations
		if err := p.pub.Publish(ctx, origEvt); err != nil {
			return fmt.Errorf("failed to republish recovered event %s: %w", origEvt.EventId, err)
		}

		return nil
	})
}
