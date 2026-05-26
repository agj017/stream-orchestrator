package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"stream-orchestrator/internal/domain"
)

type fakeRepo struct {
	claimed         []domain.OutboxEvent
	claimErr        error
	publishedCalls  []string
	failedCalls     []failedCall
	markPublishErr  error
	markFailedErr   error
}

type failedCall struct {
	id         string
	retryCount int
	available  time.Time
	lastError  string
}

func (f *fakeRepo) ClaimBatch(_ context.Context, _ int) ([]domain.OutboxEvent, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	return f.claimed, nil
}

func (f *fakeRepo) MarkPublished(_ context.Context, id string, _ time.Time) error {
	if f.markPublishErr != nil {
		return f.markPublishErr
	}
	f.publishedCalls = append(f.publishedCalls, id)
	return nil
}

func (f *fakeRepo) MarkFailed(_ context.Context, id string, retryCount int, availableAt time.Time, lastError string) error {
	if f.markFailedErr != nil {
		return f.markFailedErr
	}
	f.failedCalls = append(f.failedCalls, failedCall{id: id, retryCount: retryCount, available: availableAt, lastError: lastError})
	return nil
}

type fakePublisher struct {
	errByID map[string]error
}

func (f *fakePublisher) Publish(_ context.Context, eventType string, _ []byte) error {
	if f.errByID == nil {
		return nil
	}
	return f.errByID[eventType]
}

func TestProcessor_ProcessOnce_Success(t *testing.T) {
	repo := &fakeRepo{
		claimed: []domain.OutboxEvent{
			{ID: "1", EventType: domain.OutboxEventStreamCreated, RetryCount: 0},
		},
	}
	pub := &fakePublisher{}
	p := NewProcessor(repo, pub, Config{BatchSize: 10, PollInterval: time.Second, MaxRetry: 5})

	if err := p.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("ProcessOnce error: %v", err)
	}
	if len(repo.publishedCalls) != 1 || repo.publishedCalls[0] != "1" {
		t.Fatalf("expected published call for id=1, got %+v", repo.publishedCalls)
	}
	if len(repo.failedCalls) != 0 {
		t.Fatalf("expected 0 failed calls, got %+v", repo.failedCalls)
	}
}

func TestProcessor_ProcessOnce_PublishFail_UpdatesRetry(t *testing.T) {
	repo := &fakeRepo{
		claimed: []domain.OutboxEvent{
			{ID: "2", EventType: domain.OutboxEventStreamCreated, RetryCount: 2},
		},
	}
	pub := &fakePublisher{errByID: map[string]error{
		domain.OutboxEventStreamCreated: errors.New("mq down"),
	}}
	p := NewProcessor(repo, pub, Config{BatchSize: 10, PollInterval: time.Second, MaxRetry: 5})

	if err := p.ProcessOnce(context.Background()); err != nil {
		t.Fatalf("ProcessOnce error: %v", err)
	}
	if len(repo.publishedCalls) != 0 {
		t.Fatalf("expected 0 published calls, got %+v", repo.publishedCalls)
	}
	if len(repo.failedCalls) != 1 {
		t.Fatalf("expected 1 failed call, got %+v", repo.failedCalls)
	}
	if repo.failedCalls[0].retryCount != 3 {
		t.Fatalf("expected retry_count=3, got %d", repo.failedCalls[0].retryCount)
	}
}

