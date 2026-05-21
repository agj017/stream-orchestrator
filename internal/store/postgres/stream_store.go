package postgres

import (
	"context"
	"fmt"

	"stream-orchestrator/internal/domain"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txContextKey struct{}

type StreamStore struct {
	pool *pgxpool.Pool
}

func NewStreamStore(ctx context.Context, dbURL string) (*StreamStore, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &StreamStore{pool: pool}, nil
}

func (s *StreamStore) Close() {
	s.pool.Close()
}

func (s *StreamStore) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
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

func (s *StreamStore) InsertStream(ctx context.Context, stream domain.Stream) error {
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

func (s *StreamStore) InsertOutboxEvent(ctx context.Context, e domain.OutboxEvent) error {
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

func (s *StreamStore) exec(ctx context.Context, q string, args ...any) (pgconn.CommandTag, error) {
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
