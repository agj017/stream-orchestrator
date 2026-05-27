package instance

import (
	"context"
	"log"
	"time"

	"stream-orchestrator/internal/domain"
)

type Collector interface {
	Collect(ctx context.Context) ([]domain.StreamInstance, error)
}

type Repository interface {
	UpsertInstances(ctx context.Context, instances []domain.StreamInstance) error
	MarkStaleUnhealthy(ctx context.Context, heartbeatTimeout time.Duration) error
}

type Config struct {
	Interval         time.Duration
	HeartbeatTimeout time.Duration
}

type Reconciler struct {
	collector Collector
	repo      Repository
	cfg       Config
}

func NewReconciler(collector Collector, repo Repository, cfg Config) *Reconciler {
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 30 * time.Second
	}
	return &Reconciler{
		collector: collector,
		repo:      repo,
		cfg:       cfg,
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil {
				log.Printf("instance reconcile failed: %v", err)
			}
		}
	}
}

func (r *Reconciler) ReconcileOnce(ctx context.Context) error {
	instances, err := r.collector.Collect(ctx)
	if err != nil {
		return err
	}
	if err := r.repo.UpsertInstances(ctx, instances); err != nil {
		return err
	}
	if err := r.repo.MarkStaleUnhealthy(ctx, r.cfg.HeartbeatTimeout); err != nil {
		return err
	}
	return nil
}

