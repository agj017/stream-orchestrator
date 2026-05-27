package integration

// Taxonomy: Workflow Integration
// Scope: RabbitMQ Consumer + Provisioning Service + PostgreSQL updates.

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/events/rabbitmq"
	"stream-orchestrator/internal/provisioning"
	pgrepo "stream-orchestrator/internal/repository/postgres"
)

func TestStreamProvisionerConsumer_Integration_Success(t *testing.T) {
	dbURL := requireTestDB(t)
	rabbitURL := os.Getenv("TEST_RABBITMQ_URL")
	if rabbitURL == "" {
		t.Skip("TEST_RABBITMQ_URL is not set")
	}

	pool := openTestPool(t, dbURL)
	defer pool.Close()
	ensureSchema(t, pool)
	truncateTables(t, pool)

	streamID := "bbbbbbbb-2222-2222-2222-222222222222"
	_, err := pool.Exec(context.Background(), `
INSERT INTO streams (id, stream_key, source_url, protocol, status, created_at, updated_at)
VALUES ($1, 'cam-consumer-01', 'rtsp://example.com/live', 'rtsp', 'PENDING', NOW(), NOW())
`, streamID)
	if err != nil {
		t.Fatalf("seed stream: %v", err)
	}

	queueName := "stream.provision.integration.success"
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(queueName, false, true, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}

	body, _ := json.Marshal(map[string]string{"stream_id": streamID})
	if err := ch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Type:        domain.OutboxEventStreamCreated,
		Body:        body,
	}); err != nil {
		t.Fatalf("publish test message: %v", err)
	}

	repository, err := pgrepo.NewStreamRepository(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("new stream repository: %v", err)
	}
	defer repository.Close()

	service := provisioning.NewStreamProvisioningService(repository, provisioning.NewNoopMediaMTXProvisioner())
	consumer, err := rabbitmq.NewConsumer(rabbitURL, queueName)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Consume(ctx, func(ctx context.Context, d rabbitmq.Delivery) error {
			var p struct {
				StreamID string `json:"stream_id"`
			}
			if err := json.Unmarshal(d.Body, &p); err != nil {
				return err
			}
			return service.ProcessStreamCreated(ctx, p.StreamID)
		})
	}()

	waitForStreamStatus(t, dbURL, streamID, domain.StreamStatusRunning, 5*time.Second)
	cancel()
	<-done
}

func TestStreamProvisionerConsumer_Integration_InvalidPayload_Requeued(t *testing.T) {
	dbURL := requireTestDB(t)
	rabbitURL := os.Getenv("TEST_RABBITMQ_URL")
	if rabbitURL == "" {
		t.Skip("TEST_RABBITMQ_URL is not set")
	}

	pool := openTestPool(t, dbURL)
	defer pool.Close()
	ensureSchema(t, pool)
	truncateTables(t, pool)

	queueName := "stream.provision.integration.invalid"
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		t.Fatalf("dial rabbitmq: %v", err)
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("open channel: %v", err)
	}
	defer ch.Close()
	if _, err := ch.QueueDeclare(queueName, false, true, false, false, nil); err != nil {
		t.Fatalf("queue declare: %v", err)
	}

	if err := ch.PublishWithContext(context.Background(), "", queueName, false, false, amqp.Publishing{
		ContentType: "application/json",
		Type:        domain.OutboxEventStreamCreated,
		Body:        []byte(`{invalid-json`),
	}); err != nil {
		t.Fatalf("publish invalid payload: %v", err)
	}

	consumer, err := rabbitmq.NewConsumer(rabbitURL, queueName)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- consumer.Consume(ctx, func(_ context.Context, d rabbitmq.Delivery) error {
			var p struct {
				StreamID string `json:"stream_id"`
			}
			return json.Unmarshal(d.Body, &p)
		})
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	// The invalid message should still be available after Nack(requeue=true).
	msg, ok, err := ch.Get(queueName, true)
	if err != nil {
		t.Fatalf("queue get: %v", err)
	}
	if !ok {
		t.Fatal("expected requeued message, but queue was empty")
	}
	if msg.Type != domain.OutboxEventStreamCreated {
		t.Fatalf("unexpected message type: %s", msg.Type)
	}
}

