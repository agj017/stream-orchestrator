package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"stream-orchestrator/internal/service"
	pgrepo "stream-orchestrator/internal/repository/postgres"
	transporthttp "stream-orchestrator/internal/transport/http"
)

func TestCreateStream_Integration(t *testing.T) {
	dbURL := requireTestDB(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool := openTestPool(t, dbURL)
	defer pool.Close()

	ensureSchema(t, pool)
	truncateTables(t, pool)

	repository, err := pgrepo.NewStreamRepository(ctx, dbURL)
	if err != nil {
		t.Fatalf("NewStreamRepository: %v", err)
	}
	defer repository.Close()

	svc := service.NewStreamService(repository)
	handler := transporthttp.NewStreamHandler(svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/streams", handler.CreateStream)

	reqBody := []byte(`{"stream_key":"cam-integration-01","source_url":"rtsp://example.com/live","protocol":"rtsp"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/streams", bytes.NewReader(reqBody))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected status 201, got %d, body=%s", rr.Code, rr.Body.String())
	}

	var resp struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.ID == "" {
		t.Fatal("response id is empty")
	}
	if resp.Status != "PENDING" {
		t.Fatalf("expected PENDING status, got %s", resp.Status)
	}

	var streamCount int
	if err := pool.QueryRow(ctx, "SELECT COUNT(1) FROM streams WHERE id = $1 AND status = 'PENDING'", resp.ID).Scan(&streamCount); err != nil {
		t.Fatalf("query streams: %v", err)
	}
	if streamCount != 1 {
		t.Fatalf("expected 1 stream row, got %d", streamCount)
	}

	var eventCount int
	if err := pool.QueryRow(ctx, "SELECT COUNT(1) FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'STREAM_CREATED'", resp.ID).Scan(&eventCount); err != nil {
		t.Fatalf("query outbox_events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("expected 1 outbox event row, got %d", eventCount)
	}
}
