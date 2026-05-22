package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"stream-orchestrator/internal/domain"
	"stream-orchestrator/internal/events/outbox"
	"stream-orchestrator/internal/events/rabbitmq"
	pgstore "stream-orchestrator/internal/store/postgres"
)

func main() {
	dbURL := mustEnv("DB_URL")
	rabbitURL := mustEnv("RABBITMQ_URL")

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("failed to create db pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	exchange := envOr("RABBITMQ_EXCHANGE", "stream.events")
	pub, err := rabbitmq.NewPublisher(rabbitURL, exchange, map[string]string{
		domain.OutboxEventStreamCreated: "stream.created",
	})
	if err != nil {
		log.Fatalf("failed to create rabbitmq publisher: %v", err)
	}
	defer pub.Close()

	repo := pgstore.NewOutboxRepository(pool)
	processor := outbox.NewProcessor(repo, pub, outbox.Config{
		PollInterval: envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		BatchSize:    envInt("OUTBOX_BATCH_SIZE", 100),
		MaxRetry:     envInt("OUTBOX_MAX_RETRY", 20),
	})

	log.Printf("outbox-publisher started, exchange=%s", exchange)
	if err := processor.Run(context.Background()); err != nil {
		log.Fatalf("outbox processor stopped: %v", err)
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

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s=%s, fallback=%d", key, v, fallback)
		return fallback
	}
	return n
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("invalid %s=%s, fallback=%s", key, v, fallback.String())
		return fallback
	}
	return d
}

