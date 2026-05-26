package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/events/rabbitmq"
	"stream-orchestrator/internal/provisioning"
	pgrepo "stream-orchestrator/internal/repository/postgres"
)

type streamCreatedPayload struct {
	StreamID  string `json:"stream_id"`
	StreamKey string `json:"stream_key"`
	Protocol  string `json:"protocol"`
}

func main() {
	dbURL := mustEnv("DB_URL")
	rabbitURL := mustEnv("RABBITMQ_URL")
	queueName := envOr("RABBITMQ_PROVISION_QUEUE", "stream.provision")

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("failed to create db pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	streamRepo, err := pgrepo.NewStreamRepository(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("failed to initialize stream repository: %v", err)
	}
	defer streamRepo.Close()

	// TODO: replace with real Kubernetes MediaMTX provisioner implementation.
	provisioner := provisioning.NewNoopMediaMTXProvisioner()
	service := provisioning.NewStreamProvisioningService(streamRepo, provisioner)

	consumer, err := rabbitmq.NewConsumer(rabbitURL, queueName)
	if err != nil {
		log.Fatalf("failed to initialize rabbitmq consumer: %v", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("failed to close consumer: %v", err)
		}
	}()

	log.Printf("stream-provisioner started queue=%s", queueName)
	if err := consumer.Consume(context.Background(), func(ctx context.Context, delivery rabbitmq.Delivery) error {
		if delivery.Type != domain.OutboxEventStreamCreated {
			log.Printf("skipping unsupported event type=%s", delivery.Type)
			return nil
		}

		var payload streamCreatedPayload
		if err := json.Unmarshal(delivery.Body, &payload); err != nil {
			return err
		}
		if payload.StreamID == "" {
			return provisioning.ErrInvalidPayload
		}
		return service.ProcessStreamCreated(ctx, payload.StreamID)
	}); err != nil {
		log.Fatalf("consumer stopped with error: %v", err)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s is required", key)
	}
	return v
}

func envOr(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func contextWithTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 10*time.Second)
}

