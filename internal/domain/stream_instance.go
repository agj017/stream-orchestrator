package domain

import "time"

const (
	InstanceHealthHealthy   = "HEALTHY"
	InstanceHealthUnhealthy = "UNHEALTHY"
	InstanceHealthDraining  = "DRAINING"
)

type StreamInstance struct {
	ID                    string
	NodeName              string
	Provider              string
	K8sNamespace          string
	K8sPodName            string
	Region                string
	Zone                  string
	HealthStatus          string
	MaxStreams            int
	CurrentStreams        int
	ReservedStreams       int
	MaxBandwidthMbps      float64
	UsedBandwidthMbps     float64
	ReservedBandwidthMbps float64
	CPUUsagePct           float64
	MemUsagePct           float64
	Capabilities          []byte
	LastHeartbeatAt       time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

