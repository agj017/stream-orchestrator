package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Delivery struct {
	Type string
	Body []byte
}

type Handler func(ctx context.Context, delivery Delivery) error

type Consumer struct {
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
}

func NewConsumer(url, queueName string) (*Consumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	return &Consumer{
		conn:      conn,
		channel:   ch,
		queueName: queueName,
	}, nil
}

func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	deliveries, err := c.channel.Consume(c.queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	return c.consumeDeliveries(ctx, deliveries, handler)
}

func (c *Consumer) consumeDeliveries(ctx context.Context, deliveries <-chan amqp.Delivery, handler Handler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}
			err := handler(ctx, Delivery{
				Type: d.Type,
				Body: d.Body,
			})
			if err != nil {
				_ = d.Nack(false, true)
				continue
			}
			_ = d.Ack(false)
		}
	}
}

func (c *Consumer) Close() error {
	var retErr error
	if c.channel != nil {
		if err := c.channel.Close(); err != nil {
			retErr = err
		}
	}
	if c.conn != nil {
		if err := c.conn.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}
