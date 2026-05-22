package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/events/outbox"
	"stream-orchestrator/internal/events/rabbitmq"
	"stream-orchestrator/internal/service"
	pgrepo "stream-orchestrator/internal/repository/postgres"
	transporthttp "stream-orchestrator/internal/transport/http"
)

func TestCreateStream_E2E_APIToOutboxToRabbit(t *testing.T) {
	dbURL := requireTestDB(t)
	rabbitURL := os.Getenv("TEST_RABBITMQ_URL")
	if rabbitURL == "" {
		t.Skip("TEST_RABBITMQ_URL is not set")
	}

	pool := openTestPool(t, dbURL)
	defer pool.Close()
	ensureSchema(t, pool)
	truncateTables(t, pool)

	exchange := "stream.events.e2e"
	queue := "stream.events.e2e.q"
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

	repository, err := pgrepo.NewStreamRepository(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("new stream repository: %v", err)
	}
	defer repository.Close()

	svc := service.NewStreamService(repository)
	handler := transporthttp.NewStreamHandler(svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/streams", handler.CreateStream)

	reqBody := []byte(`{"stream_key":"cam-e2e-01","source_url":"rtsp://example.com/e2e","protocol":"rtsp"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/streams", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d body=%s", rr.Code, rr.Body.String())
	}

	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	outboxRepo := pgrepo.NewOutboxRepository(pool)
	pub, err := rabbitmq.NewPublisher(rabbitURL, exchange, map[string]string{
		domain.OutboxEventStreamCreated: routingKey,
	})
	if err != nil {
		t.Fatalf("new rabbit publisher: %v", err)
	}
	defer pub.Close()

	processor := outbox.NewProcessor(outboxRepo, pub, outbox.Config{BatchSize: 50, PollInterval: time.Second, MaxRetry: 5})
	if err := processor.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("processor ProcessOnce: %v", err)
	}

	msgs, err := ch.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	select {
	case msg := <-msgs:
		var payload map[string]string
		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			t.Fatalf("decode queue payload: %v", err)
		}
		if payload["stream_id"] != resp.ID {
			t.Fatalf("expected stream_id=%s, got %s", resp.ID, payload["stream_id"])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting queue message")
	}

	var outboxStatus string
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM outbox_events WHERE aggregate_id = $1 ORDER BY created_at DESC LIMIT 1`,
		resp.ID,
	).Scan(&outboxStatus); err != nil {
		t.Fatalf("query outbox status: %v", err)
	}
	if outboxStatus != domain.OutboxStatusPublished {
		t.Fatalf("expected outbox status PUBLISHED, got %s", outboxStatus)
	}
}
