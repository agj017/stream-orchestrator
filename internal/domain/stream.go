package domain

import "time"

const (
	StreamStatusPending = "PENDING"
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
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

