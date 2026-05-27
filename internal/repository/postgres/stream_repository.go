package postgres

import (
	"context"
	"errors"
	"fmt"

	"stream-orchestrator/internal/domain"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStreamNotFound = errors.New("stream not found")

type txContextKey struct{}

type StreamRepository struct {
	pool *pgxpool.Pool
}

func NewStreamRepository(ctx context.Context, dbURL string) (*StreamRepository, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &StreamRepository{pool: pool}, nil
}

func (s *StreamRepository) Close() {
	s.pool.Close()
}

func (s *StreamRepository) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	txCtx := context.WithValue(ctx, txContextKey{}, tx)
	if err := fn(txCtx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

func (s *StreamRepository) InsertStream(ctx context.Context, stream domain.Stream) error {
	q := `
INSERT INTO streams (
	id, stream_key, source_url, protocol, region, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`
	_, err := s.exec(ctx, q,
		stream.ID,
		stream.StreamKey,
		stream.SourceURL,
		stream.Protocol,
		nullIfEmpty(stream.Region),
		stream.Status,
		stream.CreatedAt,
		stream.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert stream: %w", err)
	}
	return nil
}

func (s *StreamRepository) InsertOutboxEvent(ctx context.Context, e domain.OutboxEvent) error {
	q := `
INSERT INTO outbox_events (
	id, aggregate_type, aggregate_id, event_type, payload, status, created_at, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
`
	_, err := s.exec(ctx, q,
		e.ID,
		e.AggregateType,
		e.AggregateID,
		e.EventType,
		e.Payload,
		e.Status,
		e.CreatedAt,
		e.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

func (s *StreamRepository) GetStreamByID(ctx context.Context, id string) (domain.Stream, error) {
	q := `
SELECT id, stream_key, source_url, protocol, COALESCE(region, ''), status, created_at, updated_at
FROM streams
WHERE id = $1
`
	var stream domain.Stream
	err := s.pool.QueryRow(ctx, q, id).Scan(
		&stream.ID,
		&stream.StreamKey,
		&stream.SourceURL,
		&stream.Protocol,
		&stream.Region,
		&stream.Status,
		&stream.CreatedAt,
		&stream.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.Stream{}, ErrStreamNotFound
		}
		return domain.Stream{}, fmt.Errorf("get stream by id: %w", err)
	}
	return stream, nil
}

func (s *StreamRepository) UpdateStreamStatus(ctx context.Context, id string, status string, failureReason *string) error {
	_, err := s.pool.Exec(ctx, `
UPDATE streams
SET status = $1, failure_reason = $2, updated_at = NOW()
WHERE id = $3
`, status, failureReason, id)
	if err != nil {
		return fmt.Errorf("update stream status: %w", err)
	}
	return nil
}

func (s *StreamRepository) exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error) {
	if tx, ok := txFromContext(ctx); ok {
		return tx.Exec(ctx, q, args...)
	}
	return s.pool.Exec(ctx, q, args...)
}

func txFromContext(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txContextKey{}).(pgx.Tx)
	return tx, ok
}

func nullIfEmpty(v string) any {
	if v == "" {
		return nil
	}
	return v
}
