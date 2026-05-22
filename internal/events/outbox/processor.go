package outbox

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"stream-orchestrator/internal/domain"
)

type Repository interface {
	ClaimBatch(ctx context.Context, batchSize int) ([]domain.OutboxEvent, error)
	MarkPublished(ctx context.Context, id string, publishedAt time.Time) error
	MarkFailed(ctx context.Context, id string, retryCount int, availableAt time.Time, lastError string) error
}

type Publisher interface {
	Publish(ctx context.Context, eventType string, payload []byte) error
}

type Config struct {
	PollInterval time.Duration
	BatchSize    int
	MaxRetry     int
}

type Processor struct {
	repo      Repository
	publisher Publisher
	cfg       Config
	now       func() time.Time
}

func NewProcessor(repo Repository, publisher Publisher, cfg Config) *Processor {
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxRetry <= 0 {
		cfg.MaxRetry = 20
	}

	return &Processor{
		repo:      repo,
		publisher: publisher,
		cfg:       cfg,
		now:       time.Now().UTC,
	}
}

func (p *Processor) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := p.ProcessOnce(ctx); err != nil {
				log.Printf("outbox process tick failed: %v", err)
			}
		}
	}
}

func (p *Processor) ProcessOnce(ctx context.Context) error {
	events, err := p.repo.ClaimBatch(ctx, p.cfg.BatchSize)
	if err != nil {
		return fmt.Errorf("claim batch: %w", err)
	}

	for _, evt := range events {
		err := p.publisher.Publish(ctx, evt.EventType, evt.Payload)
		if err == nil {
			if markErr := p.repo.MarkPublished(ctx, evt.ID, p.now()); markErr != nil {
				return fmt.Errorf("mark published id=%s: %w", evt.ID, markErr)
			}
			continue
		}

		nextRetry := evt.RetryCount + 1
		if nextRetry > p.cfg.MaxRetry {
			nextRetry = p.cfg.MaxRetry
		}
		nextAt := p.now().Add(backoffDuration(nextRetry))
		if markErr := p.repo.MarkFailed(ctx, evt.ID, nextRetry, nextAt, err.Error()); markErr != nil {
			return fmt.Errorf("mark failed id=%s: %w", evt.ID, markErr)
		}
	}

	return nil
}

func backoffDuration(retry int) time.Duration {
	seconds := math.Pow(2, float64(retry))
	if seconds > 300 {
		seconds = 300
	}
	return time.Duration(seconds) * time.Second
}
