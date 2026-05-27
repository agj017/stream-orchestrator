ALTER TABLE streams
    DROP CONSTRAINT IF EXISTS fk_streams_assigned_instance;

DROP INDEX IF EXISTS idx_streams_region_affinity;
DROP INDEX IF EXISTS idx_streams_assigned_instance_id;

ALTER TABLE streams
    DROP COLUMN IF EXISTS region_affinity,
    DROP COLUMN IF EXISTS priority,
    DROP COLUMN IF EXISTS estimated_bitrate_mbps;

DROP INDEX IF EXISTS idx_stream_instances_last_heartbeat_at;
DROP INDEX IF EXISTS idx_stream_instances_region_zone;
DROP INDEX IF EXISTS idx_stream_instances_health_status;

DROP TABLE IF EXISTS stream_instances;
