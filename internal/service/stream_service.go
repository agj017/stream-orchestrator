package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"stream-orchestrator/internal/domain"
)

var (
	ErrInvalidInput = errors.New("invalid input")
)

type CreateStreamInput struct {
	StreamKey string
	SourceURL string
	Protocol  string
	Region    string
}

type StreamRepository interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
	InsertStream(ctx context.Context, s domain.Stream) error
	InsertOutboxEvent(ctx context.Context, e domain.OutboxEvent) error
}

type StreamService struct {
	repository StreamRepository
	now   func() time.Time
}

func NewStreamService(repository StreamRepository) *StreamService {
	return &StreamService{
		repository: repository,
		now:   time.Now().UTC,
	}
}

func (s *StreamService) CreateStream(ctx context.Context, in CreateStreamInput) (domain.Stream, error) {
	if err := validateCreateStreamInput(in); err != nil {
		return domain.Stream{}, err
	}

	now := s.now()
	stream := domain.Stream{
		ID:        newID(),
		StreamKey: strings.TrimSpace(in.StreamKey),
		SourceURL: strings.TrimSpace(in.SourceURL),
		Protocol:  strings.ToLower(strings.TrimSpace(in.Protocol)),
		Region:    strings.TrimSpace(in.Region),
		Status:    domain.StreamStatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	payload, err := json.Marshal(map[string]string{
		"stream_id":  stream.ID,
		"stream_key": stream.StreamKey,
		"protocol":   stream.Protocol,
	})
	if err != nil {
		return domain.Stream{}, fmt.Errorf("marshal outbox payload: %w", err)
	}

	event := domain.OutboxEvent{
		ID:            newID(),
		AggregateType: "stream",
		AggregateID:   stream.ID,
		EventType:     "STREAM_CREATED",
		Payload:       payload,
		Status:        "PENDING",
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.repository.WithTx(ctx, func(txCtx context.Context) error {
		if err := s.repository.InsertStream(txCtx, stream); err != nil {
			return err
		}
		if err := s.repository.InsertOutboxEvent(txCtx, event); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return domain.Stream{}, err
	}

	return stream, nil
}

func validateCreateStreamInput(in CreateStreamInput) error {
	if strings.TrimSpace(in.StreamKey) == "" {
		return fmt.Errorf("%w: stream_key is required", ErrInvalidInput)
	}
	if strings.TrimSpace(in.SourceURL) == "" {
		return fmt.Errorf("%w: source_url is required", ErrInvalidInput)
	}
	u, err := url.ParseRequestURI(strings.TrimSpace(in.SourceURL))
	if err != nil || u.Scheme == "" || u.Host == "" {
		return fmt.Errorf("%w: source_url must be a valid absolute URL", ErrInvalidInput)
	}

	p := strings.ToLower(strings.TrimSpace(in.Protocol))
	switch p {
	case "rtsp", "rtmp", "webrtc", "hls":
	default:
		return fmt.Errorf("%w: unsupported protocol", ErrInvalidInput)
	}
	return nil
}

func newID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// fallback is deterministic enough for local development only
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	// RFC 4122 variant + version 4.
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", buf[0:4], buf[4:6], buf[6:8], buf[8:10], buf[10:16])
}
