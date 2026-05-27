package provisioning

import (
	"context"
	"errors"
	"testing"

	"stream-orchestrator/internal/domain"
)

type fakeStreamRepository struct {
	stream      domain.Stream
	getErr      error
	updateCalls []updateCall
	updateErr   error
}

type updateCall struct {
	id            string
	status        string
	failureReason *string
}

func (f *fakeStreamRepository) GetStreamByID(_ context.Context, _ string) (domain.Stream, error) {
	if f.getErr != nil {
		return domain.Stream{}, f.getErr
	}
	return f.stream, nil
}

func (f *fakeStreamRepository) UpdateStreamStatus(_ context.Context, id string, status string, failureReason *string) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updateCalls = append(f.updateCalls, updateCall{
		id:            id,
		status:        status,
		failureReason: failureReason,
	})
	return nil
}

type fakeProvisioner struct {
	err error
}

func (f *fakeProvisioner) ProvisionStream(_ context.Context, _ domain.Stream) error {
	return f.err
}

func TestProcessStreamCreated_Success(t *testing.T) {
	repo := &fakeStreamRepository{
		stream: domain.Stream{ID: "s1", Status: domain.StreamStatusPending},
	}
	p := &fakeProvisioner{}
	svc := NewStreamProvisioningService(repo, p)

	if err := svc.ProcessStreamCreated(context.Background(), "s1"); err != nil {
		t.Fatalf("ProcessStreamCreated error: %v", err)
	}
	if len(repo.updateCalls) != 2 {
		t.Fatalf("expected 2 status updates, got %d", len(repo.updateCalls))
	}
	if repo.updateCalls[0].status != domain.StreamStatusProvisioning {
		t.Fatalf("expected first status PROVISIONING, got %s", repo.updateCalls[0].status)
	}
	if repo.updateCalls[1].status != domain.StreamStatusRunning {
		t.Fatalf("expected second status RUNNING, got %s", repo.updateCalls[1].status)
	}
}

func TestProcessStreamCreated_ProvisionFail_SetsFailed(t *testing.T) {
	repo := &fakeStreamRepository{
		stream: domain.Stream{ID: "s2", Status: domain.StreamStatusPending},
	}
	p := &fakeProvisioner{err: errors.New("k8s error")}
	svc := NewStreamProvisioningService(repo, p)

	err := svc.ProcessStreamCreated(context.Background(), "s2")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if len(repo.updateCalls) < 2 {
		t.Fatalf("expected at least 2 status updates, got %d", len(repo.updateCalls))
	}
	last := repo.updateCalls[len(repo.updateCalls)-1]
	if last.status != domain.StreamStatusFailed {
		t.Fatalf("expected FAILED, got %s", last.status)
	}
	if last.failureReason == nil || *last.failureReason == "" {
		t.Fatal("expected failure reason to be set")
	}
}

func TestProcessStreamCreated_AlreadyRunning_NoOp(t *testing.T) {
	repo := &fakeStreamRepository{
		stream: domain.Stream{ID: "s3", Status: domain.StreamStatusRunning},
	}
	p := &fakeProvisioner{}
	svc := NewStreamProvisioningService(repo, p)

	if err := svc.ProcessStreamCreated(context.Background(), "s3"); err != nil {
		t.Fatalf("ProcessStreamCreated error: %v", err)
	}
	if len(repo.updateCalls) != 0 {
		t.Fatalf("expected no status updates, got %d", len(repo.updateCalls))
	}
}

