package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	memorystore "stream-orchestrator/internal/store/memory"
	pgstore "stream-orchestrator/internal/store/postgres"
	httptransport "stream-orchestrator/internal/transport/http"
	"stream-orchestrator/internal/service"
)

func main() {
	store := newStore()
	svc := service.NewStreamService(store)
	handler := httptransport.NewStreamHandler(svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/streams", handler.CreateStream)

	addr := ":8080"
	log.Printf("orchestrator-api started on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func newStore() service.StreamStore {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		log.Println("DB_URL is empty: using in-memory store")
		return memorystore.NewStreamStore()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := pgstore.NewStreamStore(ctx, dbURL)
	if err != nil {
		log.Printf("failed to initialize postgres store (%v): fallback to in-memory store", err)
		return memorystore.NewStreamStore()
	}
	log.Println("using postgres store")
	return store
}
