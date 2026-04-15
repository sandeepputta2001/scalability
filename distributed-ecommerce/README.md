# Distributed E-Commerce Platform

A production-grade Go service demonstrating **sharding**, **replication**, **caching**, and **event streaming** across PostgreSQL, MongoDB, Redis, and Kafka.

---

## How to Run

> Full step-by-step guide: **[RUNNING.md](RUNNING.md)**

### Prerequisites

- Docker ≥ 24 + Docker Compose ≥ 2.20
- Go ≥ 1.22
- `jq`, `curl`

### Start everything (3 commands)

```bash
make up          # build images + start all 20 containers (~30s first run)
make health      # verify API is up → {"status":"ok","shards":3}
make seed        # create admin user + 2 sample products
```

### Get a token and make your first request

```bash
# Login
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"admin@example.com","password":"password123"}' | jq -r .token)

# List products
curl -s "http://localhost:8080/api/v1/products" | jq .

# Place an order
PRODUCT_ID=$(curl -s "http://localhost:8080/api/v1/products" | jq -r '.products[0].id')
curl -s -X POST http://localhost:8080/api/v1/orders \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"items\":[{\"product_id\":\"$PRODUCT_ID\",\"quantity\":1}]}" | jq .
```

### Useful make targets

```
make up                  Start all containers
make down                Stop + remove all containers and volumes
make health              GET /health
make seed                Create sample data
make logs                Tail API logs
make worker-logs         Tail worker (Kafka consumer) logs
make kafka-ui            Open Kafka UI at http://localhost:8090
make infra-only          Start only infrastructure (no app containers)
make demo-products       List products
make demo-order          Place an order (requires TOKEN env var)
make admin-shards        PostgreSQL shard stats (requires TOKEN)
make admin-lag           Replication lag report (requires TOKEN)
make admin-outbox        Outbox event counts (requires TOKEN)
make admin-saga          Run saga with simulated failure (requires TOKEN)
make admin-all           All admin diagnostics at once (requires TOKEN)
```

### Service URLs

| Service | URL |
|---|---|
| API | http://localhost:8080 |
| Kafka UI | http://localhost:8090 |
| API docs (OpenAPI) | `docs/openapi.yaml` |
| API docs (Markdown) | `docs/API_REFERENCE.md` |

---

## Architecture

```
                         ┌──────────────────────────────────────┐
                         │           API Server (Go/Gin)        │
                         │  Auth │ Products │ Orders │ Cart     │
                         │  Rate Limit (Redis) │ JWT Auth       │
                         └──────┬──────────┬──────────┬─────────┘
                                │          │          │
              ┌─────────────────▼──┐  ┌────▼──────┐  ┌▼──────────────────┐
              │  PostgreSQL         │  │  MongoDB  │  │      Redis        │
              │  Consistent Hash    │  │  Replica  │  │  Master/Replica   │
              │  Ring (3 shards)    │  │  Set ×3   │  │  + Sentinel HA    │
              └─────────────────────┘  └───────────┘  └───────────────────┘
                                │
                         ┌──────▼──────────────────────────────────────────┐
                         │              Kafka Cluster (3 brokers)          │
                         │  KRaft mode — no ZooKeeper                      │
                         │                                                  │
                         │  Topics (partitioned + replicated):              │
                         │  order.created    [6 partitions, RF=3]          │
                         │  order.confirmed  [6 partitions, RF=3]          │
                         │  order.shipped    [6 partitions, RF=3]          │
                         │  order.cancelled  [6 partitions, RF=3]          │
                         │  stock.updated    [3 partitions, RF=3]          │
                         │  user.registered  [3 partitions, RF=3]          │
                         │  events.dlq       [1 partition,  RF=3]          │
                         └──────┬──────────────────────────────────────────┘
                                │
                         ┌──────▼──────────────────────────────────────────┐
                         │              Worker (Consumer Groups)           │
                         │  order-processor    — PG status updates         │
                         │  stock-processor    — Redis cache invalidation  │
                         │  cache-invalidator  — event-driven invalidation │
                         │  notification-svc   — email/push (stub)         │
                         │  analytics-svc      — data warehouse (stub)     │
                         │  dlq-reprocessor    — dead-letter handling      │
                         └─────────────────────────────────────────────────┘
```

---

## Key Concepts

### Kafka Partitioning (= Sharding for Kafka)
- Each topic is split into N partitions — each is an independent ordered log
- Partition key = `user_id` → all events for a user land on the same partition → per-user ordering guarantee
- `order.created` has 6 partitions → 6 consumer instances can process in parallel
- Adding consumers to a group triggers rebalance; each partition assigned to exactly one consumer

### Kafka Replication
- Replication Factor = 3: 1 leader + 2 follower replicas on different brokers
- `acks=all` (RequireAll): leader waits for all ISR replicas before confirming write
- `min.insync.replicas=2`: at least 2 replicas must ack → tolerates 1 broker failure during writes
- KRaft mode: no ZooKeeper dependency; brokers elect a controller internally

### Exactly-Once Processing
- Producer: `acks=all` + retry → at-least-once delivery
- Consumer: Redis `SET NX` idempotency check on `event_id` → deduplication
- Combined: exactly-once semantics per consumer group
- Offset committed AFTER processing (not before) → no message loss on crash

### Dead-Letter Queue
- After 3 failed retries, message routed to `events.dlq`
- DLQ consumer logs for manual inspection / alerting
- Idempotency key deleted so DLQ reprocessor can retry

### Event-Driven Cache Invalidation
- `cache-invalidator` consumer group subscribes to all data-changing topics
- Invalidates Redis keys when events arrive (more reliable than synchronous invalidation)
- Inconsistency window = Kafka consumer lag (typically <100ms)

### PostgreSQL Sharding (Consistent Hash Ring)
- 150 virtual nodes per shard → even distribution, minimal rebalancing on shard add/remove
- Shard key stored on every record → single-shard reads, no scatter-gather for user queries
- Cross-shard join: scatter-gather + application-level hash join

### Redis Caching Patterns
- Cache-aside, write-through, write-behind, singleflight, stale-while-revalidate
- Probabilistic early expiry (XFetch) prevents thundering herd
- Tag-based invalidation for bulk cache busting
- Offset caching: Kafka consumer offsets cached in Redis for fast restart

---

## Quick Start

```bash
make up       # start all infrastructure + API + worker
make health   # check API health
make seed     # create sample products
make logs     # tail API logs
make kafka-ui # open Kafka UI at http://localhost:8090
```

## API Endpoints

```
POST   /api/v1/auth/register
POST   /api/v1/auth/login

GET    /api/v1/products?category=electronics&page=1
GET    /api/v1/products/search?q=laptop
GET    /api/v1/products/:id?consistency=strong|stale|eventual
POST   /api/v1/products          (auth)

GET    /api/v1/cart              (auth)
POST   /api/v1/cart/items        (auth)
DELETE /api/v1/cart/items/:id    (auth)
DELETE /api/v1/cart              (auth)

GET    /api/v1/orders            (auth)
POST   /api/v1/orders            (auth)  → publishes order.created to Kafka
GET    /api/v1/orders/:id        (auth)

GET    /api/v1/admin/shards                  (auth)
GET    /api/v1/admin/replication-lag         (auth)
GET    /api/v1/admin/orders-with-users       (auth)
GET    /api/v1/admin/cache-inconsistencies   (auth)
GET    /api/v1/admin/cache-tag/:tag          (auth)
POST   /api/v1/admin/cache-tag/:tag/bump     (auth)

GET    /health
```

## Stack

| Component | Role |
|---|---|
| Go + Gin | API server |
| PostgreSQL ×3 | Sharded order/user storage (consistent hash ring) |
| MongoDB ×3 | Replicated product catalog (replica set) |
| Redis ×3 + Sentinel | Caching, sessions, rate limiting, distributed locks, offset cache |
| Kafka ×3 (KRaft) | Event streaming, partitioned topics, exactly-once processing |
| Kafka UI | Topic/consumer group observability at :8090 |
| Docker Compose | Local orchestration |

## File Structure

```
internal/
  kafka/
    topics.go     — topic names, partition counts, consumer group IDs
    events.go     — Envelope schema, domain event payloads
    producer.go   — partitioned producer, acks=all, Snappy compression
    consumer.go   — consumer group, idempotency, DLQ routing, offset cache
    admin.go      — topic creation with correct RF + partition count
    handlers.go   — OrderProcessor, StockProcessor, CacheInvalidator, etc.
  db/
    sharding.go         — consistent hash ring ShardManager
    consistent_hash.go  — virtual-node ring (150 vnodes/shard)
    replication_monitor.go — lag-aware read routing
    cross_shard_join.go — scatter-gather + application-level hash join
  cache/
    redis.go     — master/replica client, rate limit, distributed lock
    advanced.go  — write-through, write-behind, singleflight, XFetch, SWR
  handlers/
    order_handler.go — publishes order.created to Kafka after DB write
    auth_handler.go  — publishes user.registered to Kafka after registration
```


---

## Load Balancing, Data Partitioning, Distribution & Consistency

### Load Balancing (`internal/loadbalancer/`)

Four algorithms, all with health checking and connection tracking:

| Algorithm | File | Use Case |
|---|---|---|
| Round-Robin | `loadbalancer.go` | Homogeneous DB replicas |
| Weighted Round-Robin | `loadbalancer.go` | DB replicas with different capacities |
| Least Connections | `loadbalancer.go` | MongoDB nodes (variable request duration) |
| Consistent Hash | `loadbalancer.go` | Session affinity, cache locality |

Health checker probes every 10s; unhealthy nodes are skipped automatically.
`LoadBalancer.Do()` tracks active connections per node for least-connections routing.

### Data Partitioning (`internal/partition/`)

Four strategies with a `DistributionAnalyzer` that detects skew:

| Strategy | Use Case | Tradeoff |
|---|---|---|
| Hash | Orders by user_id, Users by email | Uniform distribution; no range queries |
| Range | Products by category (A-M, N-Z) | Range queries; hot spots on skewed data |
| Directory | Explicit key→partition mapping | Maximum flexibility; lookup table bottleneck |
| Composite | Tenant hash + range within tenant | Tenant isolation + range queries |

`DistributionAnalyzer` computes skew coefficient (stddev/mean). Skew > 0.2 triggers `rebalance_recommended=true`.

### Consistency Levels (`internal/consistency/`)

Four levels selectable per-request via `X-Consistency` header or `?consistency=` param:

| Level | Read Target | Write Concern | Use Case |
|---|---|---|---|
| `eventual` | Replica (always) | 1 node, 1s timeout | Product listings, search |
| `session` | Primary for 5s after write | 1 node, 3s timeout | Cart, profile updates |
| `bounded-staleness` | Replica if lag < 30s, else primary | 1 node, 3s timeout | Inventory display |
| `strong` | Primary (always) | Majority, 5s, fsync | Checkout, payment, orders |

The `ConsistencyMiddleware` injects the level into every request context. All repository reads call `consistency.RouteRead()` to select the correct pool.

### New Admin Endpoints

```
GET /api/v1/admin/lb-stats           — per-node health, active connections, algorithm
GET /api/v1/admin/partition-report   — distribution skew per entity type
GET /api/v1/admin/route-key?key=X    — which partition a key maps to
```

### Consistency Headers

Every response includes:
```
X-Consistency-Level: eventual|session|bounded-staleness|strong
X-Data-Source: primary|replica
X-Data-Age-Ms: <milliseconds>  (replica reads only)
```


---

## Retry Mechanisms & Transactional Outbox Pattern

### Retry Mechanisms (`internal/retry/`)

Four components, all composable:

**Exponential Backoff with Jitter** (`retry.Do`, `retry.DoWithLog`)
- Formula: `wait = min(base × 2^attempt, maxCap) × (1 + rand(0, jitter))`
- Jitter prevents thundering herd: without it, all retrying clients hit the server at the same moment
- Pre-tuned configs: `DefaultConfig`, `KafkaConfig`, `DBConfig`, `HTTPConfig`

**Circuit Breaker** (`retry.CircuitBreaker`)
- States: Closed → Open → Half-Open
- Opens after N consecutive failures; probes after resetTimeout
- While Open: all calls fail immediately (fast-fail, no network calls)
- Used on the direct Kafka publish path in `order_handler.go`

**Retry Budget** (`retry.Budget`)
- Caps total concurrent retries across all goroutines
- Prevents retry storms: N goroutines × M retries = N×M calls without a budget
- Outbox relay uses a budget of 50 concurrent Kafka retries

**Error Classification** (`retry.IsTransient`, `retry.IsTransientDB`)
- Non-retryable errors (duplicate key, permission denied, context cancelled) fail immediately
- Retryable errors (connection reset, timeout) trigger the backoff loop

### Transactional Outbox Pattern (`internal/outbox/`)

**The Problem (Dual-Write)**
```
Without outbox:
  1. INSERT order → PostgreSQL  ✓ committed
  2. CRASH
  3. Publish order.created → Kafka  ✗ never happens
  → Order exists in DB, downstream services never know
```

**The Solution**
```
With outbox:
  1. BEGIN transaction
  2. INSERT order
  3. INSERT outbox_event (same transaction)
  4. COMMIT  ← both committed atomically, or both rolled back
  5. Relay worker polls outbox_events WHERE status='pending'
  6. Relay publishes to Kafka
  7. Relay marks event as 'published'
  → Order can NEVER exist without a corresponding outbox event
```

**Relay Worker**
- Polls every 1s (configurable), batch size 100
- `SELECT FOR UPDATE SKIP LOCKED` — multiple relay instances safe to run concurrently
- Exponential backoff on failure: 1s → 2s → 4s → ... → 5min
- After 10 failures: marks event as `failed` (requires manual intervention)
- One relay per PostgreSQL shard (runs in the worker process)

**Delivery Guarantee**
- At-least-once: relay may publish twice if it crashes after Kafka ack but before marking published
- Consumer idempotency (Redis `SET NX` on `event_id`) upgrades to exactly-once

**New Admin Endpoint**
```
GET /api/v1/admin/outbox-stats  — per-shard counts: pending/published/failed
```

### Where Retry Is Applied

| Operation | Mechanism | Config |
|---|---|---|
| Fetch product from MongoDB | `retry.DoWithLog` | `DBConfig` (3 attempts, 50ms base) |
| Update stock in MongoDB | `retry.DoWithLog` | `DBConfig` |
| Direct Kafka publish | Circuit Breaker | Opens after 5 failures, resets after 30s |
| Outbox relay → Kafka | `retry.DoWithBudget` | `KafkaConfig` (5 attempts, 200ms base) |
| Kafka consumer handler | Built-in consumer retry | 3 attempts with 100ms backoff |


---

## Design Patterns — Complete Inventory

### Patterns already implemented (previous sessions)

| Pattern | Location | Purpose |
|---|---|---|
| Repository | `internal/repository/` | Abstracts DB access; hides shard routing |
| Consistent Hash Ring | `internal/db/consistent_hash.go` | Virtual-node sharding for PostgreSQL |
| Scatter-Gather | `internal/db/cross_shard_join.go` | Fan-out queries across all shards |
| Cache-Aside | `internal/cache/advanced.go` | Check cache → miss → DB → populate |
| Write-Through | `internal/cache/advanced.go` | DB write + cache write atomically |
| Write-Behind | `internal/cache/advanced.go` | Cache write → async DB write |
| Singleflight | `internal/cache/advanced.go` | One DB call for N concurrent cache misses |
| Stale-While-Revalidate | `internal/cache/advanced.go` | Serve stale, refresh async |
| Distributed Lock | `internal/cache/redis.go` | Redis SET NX for mutual exclusion |
| Rate Limiting | `internal/middleware/ratelimit.go` | Sliding-window via Redis INCR |
| Circuit Breaker | `internal/retry/retry.go` | Fast-fail on repeated failures |
| Retry + Backoff | `internal/retry/retry.go` | Exponential backoff with jitter |
| Retry Budget | `internal/retry/retry.go` | Cap total retries to prevent storms |
| Transactional Outbox | `internal/outbox/outbox.go` | Atomic DB + event write |
| Event Envelope | `internal/kafka/events.go` | Idempotency + schema versioning |
| Consumer Group | `internal/kafka/consumer.go` | Parallel event processing |
| Dead-Letter Queue | `internal/kafka/consumer.go` | Failed events routed to DLQ |
| Load Balancer (4 algos) | `internal/loadbalancer/` | RR, WRR, LeastConn, ConsistentHash |
| Data Partitioning (4 strategies) | `internal/partition/` | Hash, Range, Directory, Composite |
| Consistency Levels | `internal/consistency/` | Eventual, Session, Bounded, Strong |
| Hot-Key Detection | `internal/cache/hotkey.go` | Local shard cache for hot keys |
| Redlock | `internal/cache/cluster.go` | Multi-master distributed lock |

### Patterns added in this session (`internal/patterns/`)

| Pattern | File | Purpose |
|---|---|---|
| Builder | `builder.go` | Construct Order/Product/Query safely without telescoping constructors |
| Observer / Event Bus | `eventbus.go` | In-process pub/sub; decouples producers from consumers |
| CQRS | `cqrs.go` | Separate Command (write) and Query (read) models |
| Saga Orchestrator | `saga.go` | Distributed transactions with compensating actions |
| Bulkhead | `bulkhead.go` | Isolate resource pools; one slow service can't starve others |
| Decorator | `decorator.go` | Composable cross-cutting concerns (logging, retry, bulkhead) on repositories |
| Specification | `specification.go` | Composable query predicates (AND/OR/NOT) for MongoDB and SQL |

### New Admin Endpoints

```
GET  /api/v1/admin/bulkheads         — per-bulkhead capacity, in-use, rejections
GET  /api/v1/admin/circuit-breakers  — state (closed/open/half-open) + failure count
POST /api/v1/admin/run-saga          — run order placement saga, observe compensation
GET  /api/v1/admin/spec-demo         — build composite specification, see Mongo/SQL output
POST /api/v1/admin/cqrs-demo         — dispatch command + query, see routing
POST /api/v1/admin/builder-demo      — build Order/Product with validation
```
