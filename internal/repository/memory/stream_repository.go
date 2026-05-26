package memory

import (
	"context"
	"sync"

	"stream-orchestrator/internal/domain"
)

type StreamRepository struct {
	mu      sync.Mutex
	Streams []domain.Stream
	Events  []domain.OutboxEvent
}

func NewStreamRepository() *StreamRepository {
	return &StreamRepository{}
}

func (s *StreamRepository) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(ctx)
}

func (s *StreamRepository) InsertStream(_ context.Context, stream domain.Stream) error {
	s.Streams = append(s.Streams, stream)
	return nil
}

func (s *StreamRepository) InsertOutboxEvent(_ context.Context, e domain.OutboxEvent) error {
	s.Events = append(s.Events, e)
	return nil
}
