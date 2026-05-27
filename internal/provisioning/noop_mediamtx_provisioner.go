package provisioning

import (
	"context"

	"stream-orchestrator/internal/domain"
)

// NoopMediaMTXProvisioner is a skeleton implementation for bootstrapping.
// Replace this with a real Kubernetes/MediaMTX provisioner in the next step.
type NoopMediaMTXProvisioner struct{}

func NewNoopMediaMTXProvisioner() *NoopMediaMTXProvisioner {
	return &NoopMediaMTXProvisioner{}
}

func (p *NoopMediaMTXProvisioner) ProvisionStream(_ context.Context, _ domain.Stream) error {
	return nil
}

