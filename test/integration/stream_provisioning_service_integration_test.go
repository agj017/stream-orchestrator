package integration

// Taxonomy: Workflow Integration
// Scope: ProcessStreamCreated + PostgreSQL status transitions.

import (
	"context"
	"testing"
	"time"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/provisioning"
	pgrepo "stream-orchestrator/internal/repository/postgres"
)

func TestStreamProvisioningService_Integration_SetsRunning(t *testing.T) {
	dbURL := requireTestDB(t)
	pool := openTestPool(t, dbURL)
	defer pool.Close()

	ensureSchema(t, pool)
	truncateTables(t, pool)

	streamID := "aaaaaaaa-1111-1111-1111-111111111111"
	_, err := pool.Exec(context.Background(), `
INSERT INTO streams (id, stream_key, source_url, protocol, status, created_at, updated_at)
VALUES ($1, 'cam-provision-01', 'rtsp://example.com/live', 'rtsp', 'PENDING', NOW(), NOW())
`, streamID)
	if err != nil {
		t.Fatalf("seed stream: %v", err)
	}

	repository, err := pgrepo.NewStreamRepository(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("new stream repository: %v", err)
	}
	defer repository.Close()

	service := provisioning.NewStreamProvisioningService(
		repository,
		provisioning.NewNoopMediaMTXProvisioner(),
	)
	if err := service.ProcessStreamCreated(context.Background(), streamID); err != nil {
		t.Fatalf("ProcessStreamCreated: %v", err)
	}

	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM streams WHERE id = $1`, streamID).Scan(&status); err != nil {
		t.Fatalf("query status: %v", err)
	}
	if status != domain.StreamStatusRunning {
		t.Fatalf("expected status RUNNING, got %s", status)
	}
}

func TestStreamProvisioningService_Integration_StreamNotFound(t *testing.T) {
	dbURL := requireTestDB(t)
	pool := openTestPool(t, dbURL)
	defer pool.Close()

	ensureSchema(t, pool)
	truncateTables(t, pool)

	repository, err := pgrepo.NewStreamRepository(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("new stream repository: %v", err)
	}
	defer repository.Close()

	service := provisioning.NewStreamProvisioningService(
		repository,
		provisioning.NewNoopMediaMTXProvisioner(),
	)

	err = service.ProcessStreamCreated(context.Background(), "ffffffff-ffff-ffff-ffff-ffffffffffff")
	if err == nil {
		t.Fatal("expected error for missing stream, got nil")
	}
}

func waitForStreamStatus(t *testing.T, dbURL, streamID, expected string, timeout time.Duration) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	pool := openTestPool(t, dbURL)
	defer pool.Close()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("timeout waiting stream status=%s", expected)
		case <-ticker.C:
			var status string
			err := pool.QueryRow(context.Background(), `SELECT status FROM streams WHERE id = $1`, streamID).Scan(&status)
			if err == nil && status == expected {
				return
			}
		}
	}
}

