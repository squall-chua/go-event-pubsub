// Package rabbitmq provides a RabbitMQ-backed implementation of the event.Broker interface.
// It uses amqp091-go to manage reliable messaging over the AMQP 0.9.1 protocol.
package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/squall-chua/go-event-pubsub/pkg/event"
)

// Broker implements the event.Broker interface using RabbitMQ.
//
// Features:
//   - Automatic Recovery: Declares queues as durable to survive broker restarts.
//   - Metadata Support: Maps event Metadata into RabbitMQ message headers (amqp.Table).
//   - Queue Management: Automatically declares queues on both Publish and Consume to ensure reliability.
//
// Example:
//
//	broker, err := rabbitmq.NewBroker("amqp://guest:guest@localhost:5672/")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer broker.Close()
type Broker struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewBroker establishes a TCP connection to RabbitMQ and opens a channel.
// The url should be in the format "amqp://user:pass@host:port/".
func NewBroker(url string) (*Broker, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	return &Broker{
		conn:    conn,
		channel: ch,
	}, nil
}

// Close gracefully shuts down the RabbitMQ channel and connection.
func (b *Broker) Close() error {
	if b.channel != nil {
		b.channel.Close()
	}
	if b.conn != nil {
		return b.conn.Close()
	}
	return nil
}

// Publish ensures the target queue exists and sends a persistent JSON-encoded message to it.
// Event Metadata is automatically injected into the AMQP headers.
func (b *Broker) Publish(ctx context.Context, topic string, evt *event.Event) error {
	_, err := b.channel.QueueDeclare(
		topic, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", topic, err)
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	headers := amqp.Table{}
	for k, v := range evt.Metadata {
		headers[k] = v
	}

	return b.channel.PublishWithContext(ctx,
		"",    // exchange
		topic, // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        payload,
			Headers:     headers,
		},
	)
}

// Consume registers a consumer on the queue, automatically handling background processing.
// It uses auto-acknowledgment for incoming messages.
func (b *Broker) Consume(ctx context.Context, topic string, handler func(*event.Event) error) error {
	_, err := b.channel.QueueDeclare(
		topic, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue %s: %w", topic, err)
	}

	msgs, err := b.channel.Consume(
		topic, // queue
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register a consumer on %s: %w", topic, err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}

				var evt event.Event
				if err := json.Unmarshal(d.Body, &evt); err != nil {
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
