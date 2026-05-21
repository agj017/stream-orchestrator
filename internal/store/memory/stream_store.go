package memory

import (
	"context"
	"sync"

	"stream-orchestrator/internal/domain"
)

type StreamStore struct {
	mu      sync.Mutex
	Streams []domain.Stream
	Events  []domain.OutboxEvent
}

func NewStreamStore() *StreamStore {
	return &StreamStore{}
}

func (s *StreamStore) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fn(ctx)
}

func (s *StreamStore) InsertStream(_ context.Context, stream domain.Stream) error {
	s.Streams = append(s.Streams, stream)
	return nil
}

func (s *StreamStore) InsertOutboxEvent(_ context.Context, e domain.OutboxEvent) error {
	s.Events = append(s.Events, e)
	return nil
}

