.PHONY: run-api run-controller

run-api:
	go run ./cmd/orchestrator-api

run-controller:
	go run ./cmd/orchestrator-controller
