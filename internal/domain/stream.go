package domain

import "time"

const (
	StreamStatusPending      = "PENDING"
	StreamStatusProvisioning = "PROVISIONING"
	StreamStatusRunning      = "RUNNING"
	StreamStatusFailed       = "FAILED"

	OutboxStatusPending    = "PENDING"
	OutboxStatusProcessing = "PROCESSING"
	OutboxStatusPublished  = "PUBLISHED"
	OutboxStatusFailed     = "FAILED"

	OutboxEventStreamCreated = "STREAM_CREATED"
)

type Stream struct {
	ID        string
	StreamKey string
	SourceURL string
	Protocol  string
	Region    string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

type OutboxEvent struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       []byte
	Status        string
	RetryCount    int
	AvailableAt   time.Time
	PublishedAt   *time.Time
	LastError     string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}
