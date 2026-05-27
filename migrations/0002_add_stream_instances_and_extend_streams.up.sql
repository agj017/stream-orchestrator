CREATE TABLE IF NOT EXISTS stream_instances (
    id UUID PRIMARY KEY,
    node_name TEXT NOT NULL UNIQUE,
    provider TEXT NOT NULL,
    k8s_namespace TEXT,
    k8s_pod_name TEXT,
    region TEXT,
    zone TEXT,
    health_status TEXT NOT NULL,
    max_streams INTEGER NOT NULL DEFAULT 0,
    current_streams INTEGER NOT NULL DEFAULT 0,
    reserved_streams INTEGER NOT NULL DEFAULT 0,
    max_bandwidth_mbps NUMERIC(10,2) NOT NULL DEFAULT 0,
    used_bandwidth_mbps NUMERIC(10,2) NOT NULL DEFAULT 0,
    reserved_bandwidth_mbps NUMERIC(10,2) NOT NULL DEFAULT 0,
    cpu_usage_pct NUMERIC(5,2) NOT NULL DEFAULT 0,
    mem_usage_pct NUMERIC(5,2) NOT NULL DEFAULT 0,
    capabilities JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stream_instances_health_status
    ON stream_instances (health_status);

CREATE INDEX IF NOT EXISTS idx_stream_instances_region_zone
    ON stream_instances (region, zone);

CREATE INDEX IF NOT EXISTS idx_stream_instances_last_heartbeat_at
    ON stream_instances (last_heartbeat_at DESC);

ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS estimated_bitrate_mbps NUMERIC(10,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS region_affinity TEXT;

CREATE INDEX IF NOT EXISTS idx_streams_assigned_instance_id
    ON streams (assigned_instance_id);

CREATE INDEX IF NOT EXISTS idx_streams_region_affinity
    ON streams (region_affinity);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_streams_assigned_instance'
    ) THEN
        ALTER TABLE streams
            ADD CONSTRAINT fk_streams_assigned_instance
            FOREIGN KEY (assigned_instance_id)
            REFERENCES stream_instances(id);
    END IF;
END $$;
