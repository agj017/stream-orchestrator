package integration

import (
	"context"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/events/rabbitmq"
)

func TestRabbitMQPublisher_Integration(t *testing.T) {
	rabbitURL := os.Getenv("TEST_RABBITMQ_URL")
	if rabbitURL == "" {
		t.Skip("TEST_RABBITMQ_URL is not set")
	}

	exchange := "stream.events.integration"
	queue := "stream.events.integration.q"
	routingKey := "stream.created"

	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		t.Fatalf("amqp dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		t.Fatalf("exchange declare: %v", err)
	}
	_, err = ch.QueueDeclare(queue, false, true, false, false, nil)
	if err != nil {
		t.Fatalf("queue declare: %v", err)
	}
	if err := ch.QueueBind(queue, routingKey, exchange, false, nil); err != nil {
		t.Fatalf("queue bind: %v", err)
	}

	pub, err := rabbitmq.NewPublisher(rabbitURL, exchange, map[string]string{
		domain.OutboxEventStreamCreated: routingKey,
	})
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	defer pub.Close()

	body := []byte(`{"hello":"rabbit"}`)
	if err := pub.Publish(context.Background(), domain.OutboxEventStreamCreated, body); err != nil {
		t.Fatalf("publish: %v", err)
	}

	msgs, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}

	select {
	case msg := <-msgs:
		if string(msg.Body) != string(body) {
			t.Fatalf("unexpected body: %s", string(msg.Body))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for rabbitmq message")
	}
}

