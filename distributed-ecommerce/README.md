# Distributed E-Commerce Platform

A production-grade Go service demonstrating **sharding**, **replication**, **caching**, and **event streaming** across PostgreSQL, MongoDB, Redis, and Kafka.

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
