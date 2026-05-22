package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	exchange   string
	routingMap map[string]string
}

func NewPublisher(url, exchange string, routingMap map[string]string) (*Publisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("open channel: %w", err)
	}

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	return &Publisher{
		conn:       conn,
		channel:    ch,
		exchange:   exchange,
		routingMap: routingMap,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, eventType string, payload []byte) error {
	routingKey, ok := p.routingMap[eventType]
	if !ok {
		return fmt.Errorf("no routing key configured for event_type=%s", eventType)
	}

	return p.channel.PublishWithContext(ctx, p.exchange, routingKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Type:         eventType,
		Body:         payload,
	})
}

func (p *Publisher) Close() error {
	var retErr error
	if p.channel != nil {
		if err := p.channel.Close(); err != nil {
			retErr = err
		}
	}
	if p.conn != nil {
		if err := p.conn.Close(); err != nil && retErr == nil {
			retErr = err
		}
	}
	return retErr
}

