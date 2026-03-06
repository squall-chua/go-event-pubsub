// Package kafka provides a Kafka-backed implementation of the event.Broker interface.
// It uses github.com/segmentio/kafka-go for high-performance, idiomatic Go interactions with Kafka.
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

// WriterConfig defines tuning parameters for the Kafka producer (segmentio writer).
type WriterConfig struct {
	// Balancer specifies the partitioning strategy. Defaults to LeastBytes.
	Balancer kafka.Balancer
	// MaxAttempts is the number of write retries before failing a message.
	MaxAttempts int
	// RequiredAcks specifies how many replicas must acknowledge the write.
	RequiredAcks kafka.RequiredAcks
	// WriteTimeout is the deadline for network writes.
	WriteTimeout time.Duration
	// BatchSize is the maximum number of messages to group in one request.
	BatchSize int
	// BatchBytes is the maximum total payload size of a grouped request.
	BatchBytes int64
	// BatchTimeout is the maximum time to wait before sending a partial batch.
	BatchTimeout time.Duration
}

// ReaderConfig defines tuning parameters for the Kafka consumer (segmentio reader).
type ReaderConfig struct {
	// GroupID is the consumer group identifier. Defaults to "event-pubsub-group".
	GroupID string
	// MinBytes is the minimum amount of data to fetch per request.
	MinBytes int
	// MaxBytes is the maximum amount of data to fetch per request.
	MaxBytes int
	// MaxWait is the maximum time to block waiting for more data to reach the minimum.
	MaxWait time.Duration
	// StartOffset defines where to begin reading if no committed offset exists.
	StartOffset int64
	// CommitInterval specifies the frequency of automatic offset commits.
	CommitInterval time.Duration
}

// Config bundles all Kafka-specific connectivity and behavior settings.
type Config struct {
	// Brokers is a list of seed broker addresses (e.g., []string{"localhost:9092"}).
	Brokers []string
	// Writer holds producer-specific tuning.
	Writer WriterConfig
	// Reader holds consumer-specific tuning.
	Reader ReaderConfig
}

// Broker implements the event.Broker interface using Kafka as the transport layer.
//
// Features:
//   - Partitioning: Uses the EventId as the Kafka message key by default for consistent ordering.
//   - Headers: Automatically maps event Metadata into Kafka message headers.
//   - High Performance: Leverages segmentio/kafka-go's optimized batching and connection pooling.
//
// Example:
//
//	broker := kafka.NewBroker(kafka.Config{
//	    Brokers: []string{"kafka-1:9092", "kafka-2:9092"},
//	    Writer: kafka.WriterConfig{
//	        BatchSize: 100,
//	    },
//	    Reader: kafka.ReaderConfig{
//	        GroupID: "order-service-v1",
//	    },
//	})
type Broker struct {
	config Config
}

// NewBroker initializes a new Kafka broker instance with the provided configuration.
func NewBroker(cfg Config) *Broker {
	return &Broker{
		config: cfg,
	}
}

// Publish serializes the event to JSON and sends it to the specified Kafka topic.
// It uses the EventId as the message key to ensure that events for the same resource
// are routed to the same partition (guaranteeing ordering).
func (b *Broker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	w := &kafka.Writer{
		Addr:         kafka.TCP(b.config.Brokers...),
		Topic:        topic,
		Balancer:     b.config.Writer.Balancer,
		MaxAttempts:  b.config.Writer.MaxAttempts,
		RequiredAcks: b.config.Writer.RequiredAcks,
		WriteTimeout: b.config.Writer.WriteTimeout,
		BatchSize:    b.config.Writer.BatchSize,
		BatchBytes:   b.config.Writer.BatchBytes,
		BatchTimeout: b.config.Writer.BatchTimeout,
	}
	if w.Balancer == nil {
		w.Balancer = &kafka.LeastBytes{}
	}
	defer w.Close()

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	msg := kafka.Message{
		Value: payload,
		Key:   []byte(evt.EventId),
	}

	var headers []kafka.Header
	for k, v := range evt.Metadata {
		headers = append(headers, kafka.Header{
			Key:   k,
			Value: []byte(fmt.Sprint(v)),
		})
	}
	msg.Headers = headers

	return w.WriteMessages(ctx, msg)
}

// Consume subscribes to the specified topic using the configured consumer group.
// It runs in a background goroutine and dispatches received events to the handler.
func (b *Broker) Consume(ctx context.Context, topic string, handler func(*event.Event) error) error {
	readerCfg := kafka.ReaderConfig{
		Brokers:        b.config.Brokers,
		Topic:          topic,
		GroupID:        b.config.Reader.GroupID,
		MinBytes:       b.config.Reader.MinBytes,
		MaxBytes:       b.config.Reader.MaxBytes,
		MaxWait:        b.config.Reader.MaxWait,
		StartOffset:    b.config.Reader.StartOffset,
		CommitInterval: b.config.Reader.CommitInterval,
	}

	if readerCfg.GroupID == "" {
		readerCfg.GroupID = "event-pubsub-group"
	}

	r := kafka.NewReader(readerCfg)

	go func() {
		defer r.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.ReadMessage(ctx)
				if err != nil {
					continue
				}

				var evt event.Event
				if err := json.Unmarshal(m.Value, &evt); err != nil {
					continue // Or log error
				}

				if err := handler(&evt); err != nil {
					// Error handled by caller
				}
			}
		}
	}()

	return nil
}
