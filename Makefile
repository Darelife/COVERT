.PHONY: test test-integration test-e2e test-all build

build:
	go build ./...

# Fast path: unit tests + in-process multi-peer integration tests. Skips
# the real-subprocess e2e layer (see cmd/covert/e2e_test.go).
test:
	go test ./... -short

# Just the in-process multi-peer convergence tests (pkg/session, built on
# internal/covertest).
test-integration:
	go test ./pkg/session/... -run 'Test.*' -v

# Real `covert init`/`covert join` subprocesses, real jj commits. Slow.
test-e2e:
	go test ./cmd/covert/... -run TestE2E -v -timeout 120s

test-all:
	go test ./... -v -timeout 180s
