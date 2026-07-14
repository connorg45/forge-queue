API_URL ?= http://localhost:8080

.PHONY: bench chaos test run lint demo seed fmt-check migration-check web-build verify

bench:
	API_URL=$(API_URL) k6 run scripts/bench.js

chaos:
	API_URL=$(API_URL) scripts/chaos.sh

test:
	go test ./... -race -count=1 -cover

run:
	docker compose -f deploy/docker-compose.yml up --build

demo: run

seed:
	API_URL=$(API_URL) scripts/seed.sh

fmt-check:
	test -z "$$(gofmt -l .)"

migration-check:
	cmp migrations/0001_init.up.sql internal/store/migrations/0001_init.up.sql
	cmp migrations/0001_init.down.sql internal/store/migrations/0001_init.down.sql

web-build:
	npm --prefix web ci
	npm --prefix web run build

verify: fmt-check migration-check test web-build

lint:
	golangci-lint run
