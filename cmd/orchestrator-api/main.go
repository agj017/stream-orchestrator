package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	memoryrepo "stream-orchestrator/internal/repository/memory"
	pgrepo "stream-orchestrator/internal/repository/postgres"
	httptransport "stream-orchestrator/internal/transport/http"
	"stream-orchestrator/internal/service"
)

func main() {
	repository := newRepository()
	svc := service.NewStreamService(repository)
	handler := httptransport.NewStreamHandler(svc)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/streams", handler.CreateStream)

	addr := ":8080"
	log.Printf("orchestrator-api started on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

func newRepository() service.StreamRepository {
	dbURL := os.Getenv("DB_URL")
	if dbURL == "" {
		log.Println("DB_URL is empty: using in-memory repository")
		return memoryrepo.NewStreamRepository()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	repository, err := pgrepo.NewStreamRepository(ctx, dbURL)
	if err != nil {
		log.Printf("failed to initialize postgres repository (%v): fallback to in-memory repository", err)
		return memoryrepo.NewStreamRepository()
	}
	log.Println("using postgres repository")
	return repository
}
