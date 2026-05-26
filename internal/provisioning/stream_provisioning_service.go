package provisioning

import (
	"context"
	"errors"
	"fmt"

	"stream-orchestrator/internal/domain"
)

var ErrInvalidPayload = errors.New("invalid payload")

type StreamRepository interface {
	GetStreamByID(ctx context.Context, id string) (domain.Stream, error)
	UpdateStreamStatus(ctx context.Context, id string, status string, failureReason *string) error
}

type MediaMTXProvisioner interface {
	ProvisionStream(ctx context.Context, stream domain.Stream) error
}

type StreamProvisioningService struct {
	repository  StreamRepository
	provisioner MediaMTXProvisioner
}

func NewStreamProvisioningService(repository StreamRepository, provisioner MediaMTXProvisioner) *StreamProvisioningService {
	return &StreamProvisioningService{
		repository:  repository,
		provisioner: provisioner,
	}
}

func (s *StreamProvisioningService) ProcessStreamCreated(ctx context.Context, streamID string) error {
	stream, err := s.repository.GetStreamByID(ctx, streamID)
	if err != nil {
		return fmt.Errorf("get stream by id: %w", err)
	}

	// Idempotency: stream already running means this event has effectively been processed.
	if stream.Status == domain.StreamStatusRunning {
		return nil
	}

	if err := s.repository.UpdateStreamStatus(ctx, streamID, domain.StreamStatusProvisioning, nil); err != nil {
		return fmt.Errorf("set status provisioning: %w", err)
	}

	if err := s.provisioner.ProvisionStream(ctx, stream); err != nil {
		reason := err.Error()
		_ = s.repository.UpdateStreamStatus(ctx, streamID, domain.StreamStatusFailed, &reason)
		return fmt.Errorf("provision stream: %w", err)
	}

	if err := s.repository.UpdateStreamStatus(ctx, streamID, domain.StreamStatusRunning, nil); err != nil {
		return fmt.Errorf("set status running: %w", err)
	}
	return nil
}

