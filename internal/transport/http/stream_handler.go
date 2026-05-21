package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/service"
)

type StreamCreator interface {
	CreateStream(ctx context.Context, in service.CreateStreamInput) (domain.Stream, error)
}

type StreamHandler struct {
	svc StreamCreator
}

func NewStreamHandler(svc StreamCreator) *StreamHandler {
	return &StreamHandler{svc: svc}
}

type createStreamRequest struct {
	StreamKey string `json:"stream_key"`
	SourceURL string `json:"source_url"`
	Protocol  string `json:"protocol"`
	Region    string `json:"region"`
}

type streamResponse struct {
	ID        string `json:"id"`
	StreamKey string `json:"stream_key"`
	SourceURL string `json:"source_url"`
	Protocol  string `json:"protocol"`
	Region    string `json:"region,omitempty"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *StreamHandler) CreateStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorResponse{
			Code:    "METHOD_NOT_ALLOWED",
			Message: "method not allowed",
		})
		return
	}

	var req createStreamRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse{
			Code:    "INVALID_REQUEST",
			Message: "invalid json payload",
		})
		return
	}

	stream, err := h.svc.CreateStream(r.Context(), service.CreateStreamInput{
		StreamKey: req.StreamKey,
		SourceURL: req.SourceURL,
		Protocol:  req.Protocol,
		Region:    req.Region,
	})
	if err != nil {
		status := http.StatusInternalServerError
		code := "INTERNAL_ERROR"
		msg := "failed to create stream"
		if errors.Is(err, service.ErrInvalidInput) {
			status = http.StatusBadRequest
			code = "INVALID_REQUEST"
			msg = err.Error()
		}

		writeJSON(w, status, errorResponse{
			Code:    code,
			Message: msg,
		})
		return
	}

	writeJSON(w, http.StatusCreated, streamResponse{
		ID:        stream.ID,
		StreamKey: stream.StreamKey,
		SourceURL: stream.SourceURL,
		Protocol:  stream.Protocol,
		Region:    stream.Region,
		Status:    stream.Status,
		CreatedAt: stream.CreatedAt.Format(time.RFC3339),
		UpdatedAt: stream.UpdatedAt.Format(time.RFC3339),
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
