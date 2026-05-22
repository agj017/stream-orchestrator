package e2e

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func requireE2ETestDB(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("TEST_DB_URL")
	if dbURL == "" {
		t.Skip("TEST_DB_URL is not set")
	}
	return dbURL
}

func openE2ETestPool(t *testing.T, dbURL string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("pool.Ping: %v", err)
	}
	return pool
}

func ensureE2ESchema(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	sqlBytes, err := os.ReadFile("migrations/0001_init_streams_and_outbox.up.sql")
	if err != nil {
		sqlBytes, err = os.ReadFile("../../migrations/0001_init_streams_and_outbox.up.sql")
		if err != nil {
			t.Fatalf("read migration: %v", err)
		}
	}
	for _, stmt := range splitE2ESQLStatements(string(sqlBytes)) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := pool.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("exec schema statement: %v", err)
		}
	}
}

func truncateE2ETables(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), "TRUNCATE TABLE outbox_events, streams"); err != nil {
		t.Fatalf("truncate tables: %v", err)
	}
}

func splitE2ESQLStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s == "" {
			continue
		}
		out = append(out, s)
	}
	return out
}

