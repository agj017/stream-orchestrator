package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/service"
)

type fakeCreator struct {
	result domain.Stream
	err    error
}

func (f *fakeCreator) CreateStream(_ context.Context, _ service.CreateStreamInput) (domain.Stream, error) {
	if f.err != nil {
		return domain.Stream{}, f.err
	}
	return f.result, nil
}

func TestCreateStreamHandler_Success(t *testing.T) {
	h := NewStreamHandler(&fakeCreator{
		result: domain.Stream{
			ID:        "abc",
			StreamKey: "cam-01",
			SourceURL: "rtsp://example.com/live",
			Protocol:  "rtsp",
			Status:    domain.StreamStatusPending,
			CreatedAt: time.Date(2026, 5, 20, 1, 2, 3, 0, time.UTC),
			UpdatedAt: time.Date(2026, 5, 20, 1, 2, 3, 0, time.UTC),
		},
	})

	body := []byte(`{"stream_key":"cam-01","source_url":"rtsp://example.com/live","protocol":"rtsp"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/streams", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	h.CreateStream(rr, req)

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rr.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["status"] != domain.StreamStatusPending {
		t.Fatalf("expected status %s, got %v", domain.StreamStatusPending, resp["status"])
	}
}

func TestCreateStreamHandler_BadRequest(t *testing.T) {
	h := NewStreamHandler(&fakeCreator{
		err: fmt.Errorf("validation failed: %w", service.ErrInvalidInput),
	})

	body := []byte(`{"stream_key":"","source_url":"x","protocol":"rtsp"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/streams", bytes.NewReader(body))
	rr := httptest.NewRecorder()

	h.CreateStream(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}

func TestCreateStreamHandler_InvalidJSON(t *testing.T) {
	h := NewStreamHandler(&fakeCreator{})

	req := httptest.NewRequest(http.MethodPost, "/v1/streams", bytes.NewReader([]byte(`{`)))
	rr := httptest.NewRecorder()

	h.CreateStream(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rr.Code)
	}
}
