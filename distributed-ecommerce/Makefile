.PHONY: up down build tidy seed health logs worker-logs kafka-ui \
        ps restart clean \
        infra-only api-local worker-local \
        demo-register demo-products demo-order demo-cart \
        admin-shards admin-lag admin-outbox admin-lb admin-bulkheads \
        admin-circuit-breakers admin-saga admin-spec

# ─── Core lifecycle ───────────────────────────────────────────────────────────

## Start all containers (builds images first)
up:
	docker compose up -d --build

## Stop and remove all containers + volumes (full reset)
down:
	docker compose down -v

## Show container status
ps:
	docker compose ps

## Restart API and worker (after config changes)
restart:
	docker compose restart api worker

## Remove stopped containers and dangling images
clean:
	docker compose down
	docker system prune -f

# ─── Build ────────────────────────────────────────────────────────────────────

## Compile Go binaries locally
build:
	go build ./...

## Download / tidy dependencies
tidy:
	go mod tidy

## Build local binaries into ./bin/
bin:
	mkdir -p bin
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

# ─── Infrastructure only (no app containers) ─────────────────────────────────

## Start only infrastructure (PostgreSQL, MongoDB, Redis, Kafka) — no API/worker
infra-only:
	docker compose up -d \
		pg-primary pg-replica1 pg-replica2 \
		mongo-primary mongo-secondary1 mongo-secondary2 mongo-init \
		redis-master redis-replica1 redis-replica2 redis-sentinel1 \
		kafka-1 kafka-2 kafka-3 kafka-ui

## Run API locally against Docker infrastructure (update configs/app.yaml first)
api-local: bin
	CONFIG_PATH=configs/app.yaml ./bin/api

## Run worker locally against Docker infrastructure
worker-local: bin
	CONFIG_PATH=configs/app.yaml ./bin/worker

# ─── Observability ───────────────────────────────────────────────────────────

## Check API health
health:
	@curl -s http://localhost:8080/health | jq .

## Tail API server logs
logs:
	docker compose logs -f api

## Tail worker (Kafka consumer + outbox relay) logs
worker-logs:
	docker compose logs -f worker

## Open Kafka UI at http://localhost:8090
kafka-ui:
	@echo "Kafka UI → http://localhost:8090"
	@open http://localhost:8090 2>/dev/null || xdg-open http://localhost:8090 2>/dev/null || true

## Open API docs (requires a local HTTP server or browser)
docs:
	@echo "OpenAPI spec → distributed-ecommerce/docs/openapi.yaml"
	@echo "API Reference → distributed-ecommerce/docs/API_REFERENCE.md"
	@echo "Run guide    → distributed-ecommerce/RUNNING.md"

# ─── Seed data ────────────────────────────────────────────────────────────────

## Register admin user + create 2 sample products
seed:
	@echo "→ Registering admin user..."
	@curl -s -X POST http://localhost:8080/api/v1/auth/register \
		-H "Content-Type: application/json" \
		-d '{"email":"admin@example.com","password":"password123","name":"Admin"}' | jq .
	@echo "→ Creating products..."
	@TOKEN=$$(curl -s -X POST http://localhost:8080/api/v1/auth/login \
		-H "Content-Type: application/json" \
		-d '{"email":"admin@example.com","password":"password123"}' | jq -r .token); \
	curl -s -X POST http://localhost:8080/api/v1/products \
		-H "Authorization: Bearer $$TOKEN" \
		-H "Content-Type: application/json" \
		-d '{"name":"MacBook Pro","description":"Apple M3 laptop","price":2499.99,"stock":50,"category":"electronics","tags":["laptop","apple"]}' | jq .; \
	curl -s -X POST http://localhost:8080/api/v1/products \
		-H "Authorization: Bearer $$TOKEN" \
		-H "Content-Type: application/json" \
		-d '{"name":"Sony WH-1000XM5","description":"Noise cancelling headphones","price":349.99,"stock":200,"category":"electronics","tags":["headphones","sony"]}' | jq .

# ─── Demo flows ───────────────────────────────────────────────────────────────

## Register a demo user and print the JWT
demo-register:
	@curl -s -X POST http://localhost:8080/api/v1/auth/register \
		-H "Content-Type: application/json" \
		-d '{"email":"demo@example.com","password":"password123","name":"Demo User"}' | jq .

## List products (eventual consistency, from Redis/MongoDB replica)
demo-products:
	@curl -s "http://localhost:8080/api/v1/products?category=electronics" | jq .

## Place an order (requires TOKEN env var: export TOKEN=<jwt>)
demo-order:
	@if [ -z "$$TOKEN" ]; then \
		echo "Set TOKEN first: export TOKEN=\$$(curl -s -X POST http://localhost:8080/api/v1/auth/login -H 'Content-Type: application/json' -d '{\"email\":\"admin@example.com\",\"password\":\"password123\"}' | jq -r .token)"; \
		exit 1; \
	fi
	@PRODUCT_ID=$$(curl -s "http://localhost:8080/api/v1/products" | jq -r '.products[0].id'); \
	echo "Ordering product: $$PRODUCT_ID"; \
	curl -s -X POST http://localhost:8080/api/v1/orders \
		-H "Authorization: Bearer $$TOKEN" \
		-H "Content-Type: application/json" \
		-d "{\"items\":[{\"product_id\":\"$$PRODUCT_ID\",\"quantity\":1}]}" | jq .

## Add first product to cart (requires TOKEN env var)
demo-cart:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first (see demo-order)"; exit 1; fi
	@PRODUCT_ID=$$(curl -s "http://localhost:8080/api/v1/products" | jq -r '.products[0].id'); \
	curl -s -X POST http://localhost:8080/api/v1/cart/items \
		-H "Authorization: Bearer $$TOKEN" \
		-H "Content-Type: application/json" \
		-d "{\"product_id\":\"$$PRODUCT_ID\",\"quantity\":2}" | jq .

# ─── Admin diagnostics ───────────────────────────────────────────────────────

## PostgreSQL shard stats + ring distribution (requires TOKEN)
admin-shards:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s http://localhost:8080/api/v1/admin/shards \
		-H "Authorization: Bearer $$TOKEN" | jq .

## Replication lag per shard/replica (requires TOKEN)
admin-lag:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s http://localhost:8080/api/v1/admin/replication-lag \
		-H "Authorization: Bearer $$TOKEN" | jq .

## Outbox event counts per shard (requires TOKEN)
admin-outbox:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s http://localhost:8080/api/v1/admin/outbox-stats \
		-H "Authorization: Bearer $$TOKEN" | jq .

## Load balancer node health (requires TOKEN)
admin-lb:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s http://localhost:8080/api/v1/admin/lb-stats \
		-H "Authorization: Bearer $$TOKEN" | jq .

## Bulkhead concurrency stats (requires TOKEN)
admin-bulkheads:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s http://localhost:8080/api/v1/admin/bulkheads \
		-H "Authorization: Bearer $$TOKEN" | jq .

## Circuit breaker states (requires TOKEN)
admin-circuit-breakers:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s http://localhost:8080/api/v1/admin/circuit-breakers \
		-H "Authorization: Bearer $$TOKEN" | jq .

## Run order placement saga with failure at ChargePayment (requires TOKEN)
admin-saga:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s -X POST http://localhost:8080/api/v1/admin/run-saga \
		-H "Authorization: Bearer $$TOKEN" \
		-H "Content-Type: application/json" \
		-d '{"user_id":"550e8400-e29b-41d4-a716-446655440000","fail_at":"ChargePayment"}' | jq .

## Specification pattern demo (requires TOKEN)
admin-spec:
	@if [ -z "$$TOKEN" ]; then echo "Set TOKEN first"; exit 1; fi
	@curl -s "http://localhost:8080/api/v1/admin/spec-demo?category=electronics&tag=laptop" \
		-H "Authorization: Bearer $$TOKEN" | jq .

## Full admin dashboard — run all diagnostics at once (requires TOKEN)
admin-all: admin-shards admin-lag admin-outbox admin-lb admin-bulkheads admin-circuit-breakers
