package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/events/outbox"
	pgrepo "stream-orchestrator/internal/repository/postgres"
)

type flakyPublisher struct {
	failCount int
	calls     int
}

func (f *flakyPublisher) Publish(_ context.Context, _ string, _ []byte) error {
	f.calls++
	if f.calls <= f.failCount {
		return errors.New("temporary publish failure")
	}
	return nil
}

func TestOutboxPublisher_FailureThenRecovery_Integration(t *testing.T) {
	dbURL := requireTestDB(t)
	pool := openTestPool(t, dbURL)
	defer pool.Close()
	ensureSchema(t, pool)
	truncateTables(t, pool)

	_, err := pool.Exec(context.Background(), `
INSERT INTO outbox_events (
	id, aggregate_type, aggregate_id, event_type, payload, status, retry_count, available_at, created_at, updated_at
) VALUES
('33333333-3333-3333-3333-333333333333', 'stream', 'cccccccc-cccc-cccc-cccc-cccccccccccc', 'STREAM_CREATED', '{"a":"3"}', 'PENDING', 0, NOW(), NOW(), NOW())
`)
	if err != nil {
		t.Fatalf("seed outbox event: %v", err)
	}

	repo := pgrepo.NewOutboxRepository(pool)
	pub := &flakyPublisher{failCount: 1}
	processor := outbox.NewProcessor(repo, pub, outbox.Config{BatchSize: 10, PollInterval: time.Second, MaxRetry: 5})

	if err := processor.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once first: %v", err)
	}

	var status string
	var retryCount int
	if err := pool.QueryRow(context.Background(), `SELECT status, retry_count FROM outbox_events WHERE id = '33333333-3333-3333-3333-333333333333'`).
		Scan(&status, &retryCount); err != nil {
		t.Fatalf("query failed state: %v", err)
	}
	if status != domain.OutboxStatusFailed || retryCount != 1 {
		t.Fatalf("expected FAILED retry_count=1, got status=%s retry_count=%d", status, retryCount)
	}

	if _, err := pool.Exec(context.Background(), `UPDATE outbox_events SET available_at = NOW() WHERE id = '33333333-3333-3333-3333-333333333333'`); err != nil {
		t.Fatalf("set available_at now: %v", err)
	}

	if err := processor.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("process once second: %v", err)
	}

	if err := pool.QueryRow(context.Background(), `SELECT status, retry_count FROM outbox_events WHERE id = '33333333-3333-3333-3333-333333333333'`).
		Scan(&status, &retryCount); err != nil {
		t.Fatalf("query published state: %v", err)
	}
	if status != domain.OutboxStatusPublished {
		t.Fatalf("expected PUBLISHED, got %s", status)
	}
	if retryCount != 1 {
		t.Fatalf("expected retry_count remains 1, got %d", retryCount)
	}
}
