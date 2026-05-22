package integration

import (
	"context"
	"testing"
	"time"

	"stream-orchestrator/internal/domain"
	pgstore "stream-orchestrator/internal/store/postgres"
)

func TestOutboxRepository_ClaimAndMark_Integration(t *testing.T) {
	dbURL := requireTestDB(t)
	pool := openTestPool(t, dbURL)
	defer pool.Close()

	ensureSchema(t, pool)
	truncateTables(t, pool)

	_, err := pool.Exec(context.Background(), `
INSERT INTO outbox_events (
	id, aggregate_type, aggregate_id, event_type, payload, status, retry_count, available_at, created_at, updated_at
) VALUES
('11111111-1111-1111-1111-111111111111', 'stream', 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'STREAM_CREATED', '{"a":"1"}', 'PENDING', 0, NOW(), NOW(), NOW()),
('22222222-2222-2222-2222-222222222222', 'stream', 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'STREAM_CREATED', '{"a":"2"}', 'PENDING', 0, NOW(), NOW(), NOW())
`)
	if err != nil {
		t.Fatalf("seed outbox_events: %v", err)
	}

	repo := pgstore.NewOutboxRepository(pool)
	events, err := repo.ClaimBatch(context.Background(), 1)
	if err != nil {
		t.Fatalf("ClaimBatch: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 claimed event, got %d", len(events))
	}

	var status string
	if err := pool.QueryRow(context.Background(), "SELECT status FROM outbox_events WHERE id=$1", events[0].ID).Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != domain.OutboxStatusProcessing {
		t.Fatalf("expected status PROCESSING, got %s", status)
	}

	if err := repo.MarkPublished(context.Background(), events[0].ID, time.Now().UTC()); err != nil {
		t.Fatalf("MarkPublished: %v", err)
	}
	if err := pool.QueryRow(context.Background(), "SELECT status FROM outbox_events WHERE id=$1", events[0].ID).Scan(&status); err != nil {
		t.Fatalf("query published status: %v", err)
	}
	if status != domain.OutboxStatusPublished {
		t.Fatalf("expected status PUBLISHED, got %s", status)
	}
}

