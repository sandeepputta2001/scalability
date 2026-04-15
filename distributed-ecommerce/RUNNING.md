# How to Run — Distributed E-Commerce

Complete step-by-step guide from zero to a fully running distributed system.

---

## Prerequisites

Install these before starting:

| Tool | Version | Install |
|---|---|---|
| Docker | ≥ 24 | https://docs.docker.com/get-docker/ |
| Docker Compose | ≥ 2.20 (bundled with Docker Desktop) | included above |
| Go | ≥ 1.22 | https://go.dev/dl/ |
| `jq` | any | `brew install jq` / `apt install jq` |
| `curl` | any | pre-installed on macOS/Linux |

Verify:
```bash
docker --version          # Docker version 24+
docker compose version    # Docker Compose version v2.20+
go version                # go1.22+
jq --version              # jq-1.6+
```

---

## Quick Start (3 commands)

```bash
git clone <repo-url>
cd distributed-ecommerce
make up          # build images + start all 20 containers
```

Wait ~30 seconds for all services to initialise, then:

```bash
make health      # should print {"status":"ok","shards":3,...}
make seed        # create a user + 2 sample products
```

---

## What `make up` starts

| Container | Role | Port |
|---|---|---|
| `api` | Go HTTP server | 8080 |
| `worker` | Kafka consumers + outbox relay | — |
| `pg-primary` | PostgreSQL shard 0 (write) | 5432 |
| `pg-replica1` | PostgreSQL shard 1 | 5433 |
| `pg-replica2` | PostgreSQL shard 2 | 5434 |
| `mongo-primary` | MongoDB primary | 27017 |
| `mongo-secondary1` | MongoDB secondary | 27018 |
| `mongo-secondary2` | MongoDB secondary | 27019 |
| `mongo-init` | Initialises replica set (exits after) | — |
| `redis-master` | Redis master | 6379 |
| `redis-replica1` | Redis replica | 6380 |
| `redis-replica2` | Redis replica | 6381 |
| `redis-sentinel1` | Redis Sentinel (HA) | — |
| `redis-cluster-1..6` | Redis Cluster (6 nodes) | 7001–7006 |
| `redis-cluster-init` | Initialises cluster topology (exits after) | — |
| `kafka-1` | Kafka broker 1 (KRaft) | 9092 |
| `kafka-2` | Kafka broker 2 | 9093 |
| `kafka-3` | Kafka broker 3 | 9094 |
| `kafka-ui` | Kafka web UI | 8090 |

---

## Step-by-Step Walkthrough

### Step 1 — Clone and enter the project

```bash
git clone <repo-url>
cd distributed-ecommerce
```

### Step 2 — (Optional) Build locally first to catch errors early

```bash
go mod tidy          # download dependencies
go build ./...       # compile everything — should produce no errors
```

### Step 3 — Start all infrastructure

```bash
make up
# equivalent to: docker compose up -d --build
```

This builds the Go binaries inside Docker and starts all containers.
First run takes 2–5 minutes to pull images.

### Step 4 — Wait for services to be ready

```bash
# Watch container status
docker compose ps

# All containers should show "running" or "exited" (init containers exit after setup)
# mongo-init and redis-cluster-init will show "exited 0" — that's correct
```

Check the API is up:
```bash
make health
# Expected:
# {
#   "status": "ok",
#   "shards": 3,
#   "service": "distributed-ecommerce"
# }
```

If health returns an error, check logs:
```bash
make logs           # API server logs
make worker-logs    # Worker / Kafka consumer logs
docker compose logs mongo-init          # MongoDB replica set init
docker compose logs redis-cluster-init  # Redis cluster init
```

### Step 5 — Seed sample data

```bash
make seed
```

This:
1. Registers `admin@example.com` (password: `password123`)
2. Logs in and gets a JWT
3. Creates 2 products (MacBook Pro, Sony headphones)

### Step 6 — Open the Kafka UI

```bash
make kafka-ui
# Opens http://localhost:8090 in your browser
```

You can see topics, partitions, consumer groups, and message offsets.

---

## Manual API Walkthrough

### Register and get a token

```bash
# Register
curl -s -X POST http://localhost:8080/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"bob@example.com","password":"password123","name":"Bob"}' | jq .

# Login and save token
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"bob@example.com","password":"password123"}' | jq -r .token)

echo "Token: $TOKEN"
```

### Browse products

```bash
# List all products
curl -s "http://localhost:8080/api/v1/products" | jq .

# List electronics
curl -s "http://localhost:8080/api/v1/products?category=electronics" | jq .

# Search
curl -s "http://localhost:8080/api/v1/products/search?q=laptop" | jq .

# Get one product (save the ID from the list above)
PRODUCT_ID="<id-from-list>"
curl -s "http://localhost:8080/api/v1/products/$PRODUCT_ID" | jq .

# Get with strong consistency (always hits MongoDB primary)
curl -s "http://localhost:8080/api/v1/products/$PRODUCT_ID?consistency=strong" | jq .

# Get with stale-while-revalidate
curl -s "http://localhost:8080/api/v1/products/$PRODUCT_ID?consistency=stale" | jq .
```

### Cart operations

```bash
# Add to cart
curl -s -X POST http://localhost:8080/api/v1/cart/items \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"product_id\":\"$PRODUCT_ID\",\"quantity\":2}" | jq .

# View cart
curl -s http://localhost:8080/api/v1/cart \
  -H "Authorization: Bearer $TOKEN" | jq .

# Remove item
curl -s -X DELETE "http://localhost:8080/api/v1/cart/items/$PRODUCT_ID" \
  -H "Authorization: Bearer $TOKEN" | jq .

# Clear cart
curl -s -X DELETE http://localhost:8080/api/v1/cart \
  -H "Authorization: Bearer $TOKEN" | jq .
```

### Place an order

```bash
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "X-Correlation-ID: $(uuidgen 2>/dev/null || cat /proc/sys/kernel/random/uuid)" \
  -d "{\"items\":[{\"product_id\":\"$PRODUCT_ID\",\"quantity\":1}]}" | jq .

# List orders (use session consistency to see the new order immediately)
curl -s http://localhost:8080/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Consistency: session" \
  -H "X-Session-Token: bob-session-1" | jq .
```

---

## Admin Endpoints Walkthrough

All admin endpoints require the JWT token.

```bash
# PostgreSQL shard stats + ring distribution
curl -s http://localhost:8080/api/v1/admin/shards \
  -H "Authorization: Bearer $TOKEN" | jq .

# Replication lag per shard/replica
curl -s http://localhost:8080/api/v1/admin/replication-lag \
  -H "Authorization: Bearer $TOKEN" | jq .

# Cross-shard join (scatter-gather all shards)
curl -s "http://localhost:8080/api/v1/admin/orders-with-users?limit=10" \
  -H "Authorization: Bearer $TOKEN" | jq .

# Outbox event stats (pending/published/failed per shard)
curl -s http://localhost:8080/api/v1/admin/outbox-stats \
  -H "Authorization: Bearer $TOKEN" | jq .

# Load balancer node health
curl -s http://localhost:8080/api/v1/admin/lb-stats \
  -H "Authorization: Bearer $TOKEN" | jq .

# Data partition distribution + skew
curl -s http://localhost:8080/api/v1/admin/partition-report \
  -H "Authorization: Bearer $TOKEN" | jq .

# Route a key to its partition
curl -s "http://localhost:8080/api/v1/admin/route-key?key=bob@example.com&entity=user" \
  -H "Authorization: Bearer $TOKEN" | jq .

# Cache inconsistency log
curl -s http://localhost:8080/api/v1/admin/cache-inconsistencies \
  -H "Authorization: Bearer $TOKEN" | jq .

# Bulkhead stats
curl -s http://localhost:8080/api/v1/admin/bulkheads \
  -H "Authorization: Bearer $TOKEN" | jq .

# Circuit breaker states
curl -s http://localhost:8080/api/v1/admin/circuit-breakers \
  -H "Authorization: Bearer $TOKEN" | jq .

# Run a saga (no failure)
curl -s -X POST http://localhost:8080/api/v1/admin/run-saga \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"550e8400-e29b-41d4-a716-446655440000"}' | jq .

# Run a saga with simulated failure at ChargePayment
curl -s -X POST http://localhost:8080/api/v1/admin/run-saga \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"user_id":"550e8400-e29b-41d4-a716-446655440000","fail_at":"ChargePayment"}' | jq .

# Specification pattern demo
curl -s "http://localhost:8080/api/v1/admin/spec-demo?category=electronics&tag=laptop" \
  -H "Authorization: Bearer $TOKEN" | jq .

# Redis cluster stats (standalone mode returns mode:standalone)
curl -s http://localhost:8080/api/v1/admin/redis-cluster \
  -H "Authorization: Bearer $TOKEN" | jq .

# Redis slot for a key
curl -s "http://localhost:8080/api/v1/admin/redis-slot?key={product:123}:data" \
  -H "Authorization: Bearer $TOKEN" | jq .
```

---

## Switching to Redis Cluster Mode

By default the app uses standalone Redis (master + 2 replicas + Sentinel).
To switch to Redis Cluster (sharded, 3 masters × 1 replica):

1. Edit `configs/app.yaml`:
```yaml
redis:
  cluster:
    enabled: true   # change false → true
```

2. Restart the API:
```bash
docker compose restart api worker
```

The Redis Cluster nodes (`redis-cluster-1` through `redis-cluster-6`) are
already running. The `redis-cluster-init` container already initialised the
topology on first `make up`.

---

## Stopping and Cleaning Up

```bash
# Stop all containers (keep volumes / data)
docker compose stop

# Stop and remove containers (keep volumes)
docker compose down

# Stop, remove containers AND all data volumes (full reset)
make down
# equivalent to: docker compose down -v
```

---

## Running Without Docker (local Go binary)

If you want to run the Go binary directly against local infrastructure:

### 1. Start only infrastructure

```bash
docker compose up -d \
  pg-primary pg-replica1 pg-replica2 \
  mongo-primary mongo-secondary1 mongo-secondary2 mongo-init \
  redis-master redis-replica1 redis-replica2 redis-sentinel1 \
  kafka-1 kafka-2 kafka-3 kafka-ui
```

### 2. Update config for localhost addresses

Edit `configs/app.yaml` — change all service hostnames to `localhost`:

```yaml
postgres:
  shards:
    - id: 0
      primary: "postgres://appuser:secret@localhost:5432/orders_shard0?sslmode=disable"
      replicas:
        - "postgres://appuser:secret@localhost:5432/orders_shard0?sslmode=disable"
    - id: 1
      primary: "postgres://appuser:secret@localhost:5433/orders_shard1?sslmode=disable"
      replicas:
        - "postgres://appuser:secret@localhost:5433/orders_shard1?sslmode=disable"
    - id: 2
      primary: "postgres://appuser:secret@localhost:5434/orders_shard2?sslmode=disable"
      replicas:
        - "postgres://appuser:secret@localhost:5434/orders_shard2?sslmode=disable"

mongodb:
  uri: "mongodb://mongouser:secret@localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0&authSource=admin"

redis:
  master:
    addr: "localhost:6379"

kafka:
  brokers:
    - "localhost:9092"
    - "localhost:9093"
    - "localhost:9094"
```

### 3. Build and run

```bash
# Build
go build -o bin/api ./cmd/api
go build -o bin/worker ./cmd/worker

# Run API server
CONFIG_PATH=configs/app.yaml ./bin/api

# In a second terminal, run the worker
CONFIG_PATH=configs/app.yaml ./bin/worker
```

---

## Troubleshooting

### API returns 500 on startup

The API runs DB migrations on startup. If PostgreSQL isn't ready yet:
```bash
docker compose logs api | tail -20
# Look for "migrations applied" — if missing, PostgreSQL wasn't ready
docker compose restart api
```

### MongoDB replica set not initialised

```bash
docker compose logs mongo-init
# Should end with "MongoDB replica set initialized and indexes created."
# If it failed, re-run:
docker compose run --rm mongo-init
```

### Kafka topics not created

```bash
docker compose logs api | grep -i kafka
# The API creates topics on startup. If Kafka wasn't ready:
docker compose restart api worker
```

### Redis Cluster init failed

```bash
docker compose logs redis-cluster-init
# If it failed with "ERR This instance has cluster support disabled":
# The cluster nodes need --cluster-enabled yes (already set in docker-compose.yml)
# Re-run:
docker compose run --rm redis-cluster-init
```

### Port conflicts

If any port is already in use on your machine:

```bash
# Find what's using port 5432
lsof -i :5432

# Or change the host port in docker-compose.yml:
# ports:
#   - "15432:5432"   # use 15432 on host instead
```

### Full reset

```bash
make down                    # removes containers + volumes
docker system prune -f       # removes dangling images
make up                      # fresh start
```

---

## Makefile Reference

```
make up              Start all containers (build images first)
make down            Stop and remove all containers + volumes
make build           Compile Go binaries locally
make tidy            Run go mod tidy
make seed            Register admin user + create 2 sample products
make health          GET /health and print result
make logs            Tail API server logs
make worker-logs     Tail worker logs
make kafka-ui        Open Kafka UI at http://localhost:8090
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| API | http://localhost:8080 | JWT from /auth/login |
| Kafka UI | http://localhost:8090 | none |
| PostgreSQL shard 0 | localhost:5432 | appuser / secret |
| PostgreSQL shard 1 | localhost:5433 | appuser / secret |
| PostgreSQL shard 2 | localhost:5434 | appuser / secret |
| MongoDB primary | localhost:27017 | mongouser / secret |
| Redis master | localhost:6379 | password: secret |
| Kafka broker 1 | localhost:9092 | none |
| Kafka broker 2 | localhost:9093 | none |
| Kafka broker 3 | localhost:9094 | none |
