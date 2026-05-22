package postgres

import (
	"context"
	"fmt"
	"time"

	"stream-orchestrator/internal/domain"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type OutboxRepository struct {
	pool *pgxpool.Pool
}

func NewOutboxRepository(pool *pgxpool.Pool) *OutboxRepository {
	return &OutboxRepository{pool: pool}
}

func (r *OutboxRepository) ClaimBatch(ctx context.Context, batchSize int) ([]domain.OutboxEvent, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := `
SELECT id, aggregate_type, aggregate_id, event_type, payload, status, retry_count, available_at, created_at, updated_at
FROM outbox_events
WHERE status IN ($1, $2)
  AND available_at <= NOW()
ORDER BY created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT $3
`
	rows, err := tx.Query(ctx, q, domain.OutboxStatusPending, domain.OutboxStatusFailed, batchSize)
	if err != nil {
		return nil, fmt.Errorf("query claim batch: %w", err)
	}
	defer rows.Close()

	events := make([]domain.OutboxEvent, 0, batchSize)
	ids := make([]string, 0, batchSize)
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(
			&e.ID,
			&e.AggregateType,
			&e.AggregateID,
			&e.EventType,
			&e.Payload,
			&e.Status,
			&e.RetryCount,
			&e.AvailableAt,
			&e.CreatedAt,
			&e.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox row: %w", err)
		}
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	if len(ids) > 0 {
		if _, err := tx.Exec(ctx,
			`UPDATE outbox_events SET status = $1, updated_at = NOW() WHERE id = ANY($2::uuid[])`,
			domain.OutboxStatusProcessing,
			ids,
		); err != nil {
			return nil, fmt.Errorf("mark processing: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}
	return events, nil
}

func (r *OutboxRepository) MarkPublished(ctx context.Context, id string, publishedAt time.Time) error {
	_, err := r.pool.Exec(ctx, `
UPDATE outbox_events
SET status = $1, published_at = $2, last_error = NULL, updated_at = NOW()
WHERE id = $3
`, domain.OutboxStatusPublished, publishedAt, id)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}
	return nil
}

func (r *OutboxRepository) MarkFailed(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error {
	_, err := r.pool.Exec(ctx, `
UPDATE outbox_events
SET status = $1, retry_count = $2, available_at = $3, last_error = $4, updated_at = NOW()
WHERE id = $5
`, domain.OutboxStatusFailed, retryCount, availableAt, lastError, id)
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}
	return nil
}

