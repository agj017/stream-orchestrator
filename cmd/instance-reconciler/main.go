package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pgrepo "stream-orchestrator/internal/repository/postgres"
	instreconciler "stream-orchestrator/internal/reconciler/instance"
)

func main() {
	dbURL := mustEnv("DB_URL")

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("failed to create db pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(context.Background()); err != nil {
		log.Fatalf("failed to ping db: %v", err)
	}

	repo := pgrepo.NewStreamInstanceRepository(pool)
	collector := instreconciler.NewNoopCollector()

	reconciler := instreconciler.NewReconciler(collector, repo, instreconciler.Config{
		Interval:         envDuration("RECONCILE_INTERVAL", 10*time.Second),
		HeartbeatTimeout: envDuration("HEARTBEAT_TIMEOUT", 30*time.Second),
	})

	log.Printf("instance-reconciler started interval=%s heartbeat_timeout=%s",
		envDuration("RECONCILE_INTERVAL", 10*time.Second),
		envDuration("HEARTBEAT_TIMEOUT", 30*time.Second),
	)
	if err := reconciler.Run(context.Background()); err != nil {
		log.Fatalf("instance-reconciler stopped: %v", err)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("%s is required", key)
	}
	return v
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	// allow either duration string (e.g. 10s) or integer seconds.
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	if sec, err := strconv.Atoi(v); err == nil {
		return time.Duration(sec) * time.Second
	}
	return fallback
}

