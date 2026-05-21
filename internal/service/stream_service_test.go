package service

import (
	"context"
	"errors"
	"testing"

	"stream-orchestrator/internal/domain"
)

type fakeStore struct {
	streams      []domain.Stream
	events       []domain.OutboxEvent
	insertErr    error
	insertEvtErr error
}

func (f *fakeStore) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

func (f *fakeStore) InsertStream(_ context.Context, s domain.Stream) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	f.streams = append(f.streams, s)
	return nil
}

func (f *fakeStore) InsertOutboxEvent(_ context.Context, e domain.OutboxEvent) error {
	if f.insertEvtErr != nil {
		return f.insertEvtErr
	}
	f.events = append(f.events, e)
	return nil
}

func TestCreateStream_Success(t *testing.T) {
	store := &fakeStore{}
	svc := NewStreamService(store)

	stream, err := svc.CreateStream(context.Background(), CreateStreamInput{
		StreamKey: "cam-01",
		SourceURL: "rtsp://example.com/live",
		Protocol:  "rtsp",
		Region:    "ap-northeast-2",
	})
	if err != nil {
		t.Fatalf("CreateStream error: %v", err)
	}

	if stream.Status != domain.StreamStatusPending {
		t.Fatalf("expected status %s, got %s", domain.StreamStatusPending, stream.Status)
	}
	if len(store.streams) != 1 {
		t.Fatalf("expected 1 stream insert, got %d", len(store.streams))
	}
	if len(store.events) != 1 {
		t.Fatalf("expected 1 outbox event insert, got %d", len(store.events))
	}
	if store.events[0].EventType != "STREAM_CREATED" {
		t.Fatalf("expected event type STREAM_CREATED, got %s", store.events[0].EventType)
	}
}

func TestCreateStream_InvalidInput(t *testing.T) {
	store := &fakeStore{}
	svc := NewStreamService(store)

	_, err := svc.CreateStream(context.Background(), CreateStreamInput{
		StreamKey: "",
		SourceURL: "bad-url",
		Protocol:  "xxx",
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("expected ErrInvalidInput, got %v", err)
	}
}

