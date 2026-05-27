package postgres

import (
	"context"
	"fmt"
	"time"

	"stream-orchestrator/internal/domain"

	"github.com/jackc/pgx/v5/pgxpool"
)

type StreamInstanceRepository struct {
	pool *pgxpool.Pool
}

func NewStreamInstanceRepository(pool *pgxpool.Pool) *StreamInstanceRepository {
	return &StreamInstanceRepository{pool: pool}
}

func (r *StreamInstanceRepository) UpsertInstances(ctx context.Context, instances []domain.StreamInstance) error {
	if len(instances) == 0 {
		return nil
	}

	q := `
INSERT INTO stream_instances (
    id, node_name, provider, k8s_namespace, k8s_pod_name, region, zone, health_status,
    max_streams, current_streams, reserved_streams, max_bandwidth_mbps, used_bandwidth_mbps,
    reserved_bandwidth_mbps, cpu_usage_pct, mem_usage_pct, capabilities, last_heartbeat_at,
    created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, $10, $11, $12, $13,
    $14, $15, $16, $17, $18,
    NOW(), NOW()
)
ON CONFLICT (id) DO UPDATE SET
    node_name = EXCLUDED.node_name,
    provider = EXCLUDED.provider,
    k8s_namespace = EXCLUDED.k8s_namespace,
    k8s_pod_name = EXCLUDED.k8s_pod_name,
    region = EXCLUDED.region,
    zone = EXCLUDED.zone,
    health_status = EXCLUDED.health_status,
    max_streams = EXCLUDED.max_streams,
    current_streams = EXCLUDED.current_streams,
    reserved_streams = EXCLUDED.reserved_streams,
    max_bandwidth_mbps = EXCLUDED.max_bandwidth_mbps,
    used_bandwidth_mbps = EXCLUDED.used_bandwidth_mbps,
    reserved_bandwidth_mbps = EXCLUDED.reserved_bandwidth_mbps,
    cpu_usage_pct = EXCLUDED.cpu_usage_pct,
    mem_usage_pct = EXCLUDED.mem_usage_pct,
    capabilities = EXCLUDED.capabilities,
    last_heartbeat_at = EXCLUDED.last_heartbeat_at,
    updated_at = NOW()
`

	for _, inst := range instances {
		if _, err := r.pool.Exec(ctx, q,
			inst.ID,
			inst.NodeName,
			inst.Provider,
			nullIfEmpty(inst.K8sNamespace),
			nullIfEmpty(inst.K8sPodName),
			nullIfEmpty(inst.Region),
			nullIfEmpty(inst.Zone),
			inst.HealthStatus,
			inst.MaxStreams,
			inst.CurrentStreams,
			inst.ReservedStreams,
			inst.MaxBandwidthMbps,
			inst.UsedBandwidthMbps,
			inst.ReservedBandwidthMbps,
			inst.CPUUsagePct,
			inst.MemUsagePct,
			inst.Capabilities,
			inst.LastHeartbeatAt,
		); err != nil {
			return fmt.Errorf("upsert stream_instance id=%s: %w", inst.ID, err)
		}
	}
	return nil
}

func (r *StreamInstanceRepository) MarkStaleUnhealthy(ctx context.Context, heartbeatTimeout time.Duration) error {
	if heartbeatTimeout <= 0 {
		return nil
	}
	cutoff := time.Now().UTC().Add(-heartbeatTimeout)
	_, err := r.pool.Exec(ctx, `
UPDATE stream_instances
SET health_status = $1, updated_at = NOW()
WHERE last_heartbeat_at < $2
  AND health_status <> $1
`, domain.InstanceHealthUnhealthy, cutoff)
	if err != nil {
		return fmt.Errorf("mark stale instances unhealthy: %w", err)
	}
	return nil
}

