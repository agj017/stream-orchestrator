package instance

import (
	"context"

	"stream-orchestrator/internal/domain"
)

// NoopCollector is a skeleton collector.
// Replace with Kubernetes/MediaMTX-backed collector in the next step.
type NoopCollector struct{}

func NewNoopCollector() *NoopCollector {
	return &NoopCollector{}
}

func (c *NoopCollector) Collect(_ context.Context) ([]domain.StreamInstance, error) {
	return []domain.StreamInstance{}, nil
}

