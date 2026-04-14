# Distributed E-Commerce — HLD & LLD

---

## High-Level Design (HLD)

### System Overview

```
Clients (Web / Mobile / Admin)
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    API Server  (Go / Gin)                       │
│  Auth │ Products │ Orders │ Cart │ Admin                        │
│  Middleware: JWT Auth │ Redis Rate Limit │ Recovery             │
└──────┬──────────────┬──────────────┬──────────────┬────────────┘
       │              │              │              │
       ▼              ▼              ▼              ▼
┌────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
│ PostgreSQL │  │   MongoDB    │  │    Redis     │  │       Kafka          │
│  3 Shards  │  │  Replica Set │  │  M/R/Sentinel│  │  3-Broker KRaft      │
│  Hash Ring │  │  ×3 nodes    │  │  ×3 nodes    │  │  Cluster             │
└────────────┘  └──────────────┘  └──────────────┘  └──────────┬───────────┘
                                                                │
                                                     ┌──────────▼───────────┐
                                                     │  Worker Process      │
                                                     │  Consumer Groups:    │
                                                     │  order-processor     │
                                                     │  stock-processor     │
                                                     │  cache-invalidator   │
                                                     │  notification-svc    │
                                                     │  analytics-svc       │
                                                     │  dlq-reprocessor     │
                                                     └──────────────────────┘
```

### Data Flow: POST /api/v1/orders

```
1. API: AcquireLock(Redis)          — prevent duplicate submission
2. API: GetByID(MongoDB)            — fetch product price/stock
3. API: INSERT orders (PostgreSQL)  — write to correct shard (primary)
4. API: UpdateStock(MongoDB)        — decrement stock (write concern: majority)
5. API: DEL product:{id} (Redis)    — synchronous cache invalidation
6. API: DEL cart:{user} (Redis)
7. API: ReleaseLock(Redis)
8. API: Publish order.created → Kafka (partition key = user_id)
         │
         ▼ (async, independent of HTTP response)
9. Worker/order-processor:    update order status in PostgreSQL
10. Worker/cache-invalidator: DEL order:{id} in Redis
11. Worker/notification-svc:  send order confirmation email
12. Worker/analytics-svc:     write event to data warehouse
```

### Data Partitioning

| Data | Storage | Sharding Key | Rationale |
|---|---|---|---|
| Users | PostgreSQL | email hash | Uniform; login always knows email |
| Orders | PostgreSQL | user_id hash | All orders for user on one shard |
| Products | MongoDB | None (replica) | Read-heavy; replication > sharding |
| Carts | Redis | user_id | Ephemeral; TTL-based |
| Sessions | Redis | token | Ephemeral; fast lookup |
| Events | Kafka | user_id | Per-user ordering guarantee |

---

## Low-Level Design (LLD)

### 1. Kafka — Partitioning, Replication, Consistency

**Files:** `internal/kafka/`

#### 1a. Partitioning (= Sharding for Kafka)

```
Topic: order.created  [6 partitions, RF=3]

Partition assignment (murmur2 hash of key):
  user_id "abc..." → hash → partition 2
  user_id "xyz..." → hash → partition 5

All events for user "abc..." always land on partition 2.
→ Strict per-user event ordering guaranteed.

Consumer group "order-processor" with 6 instances:
  instance 0 → partition 0
  instance 1 → partition 1
  ...
  instance 5 → partition 5
  → 6-way parallel processing, no coordination needed
```

Partition count design:
| Topic | Partitions | Rationale |
|---|---|---|
| order.created/confirmed/shipped/cancelled | 6 | High volume; 6-way parallelism |
| stock.updated | 3 | Medium volume |
| user.registered | 3 | Low volume |
| events.dlq | 1 | Low volume; ordering matters for replay |

#### 1b. Replication

```
Replication Factor = 3 (1 leader + 2 followers)

Broker layout for partition 0 of order.created:
  kafka-1: LEADER   ← producer writes here
  kafka-2: FOLLOWER ← replicates from leader
  kafka-3: FOLLOWER ← replicates from leader

acks=all (RequireAll):
  Producer → kafka-1 (leader)
  kafka-1 waits for kafka-2 AND kafka-3 to ack
  Only then confirms to producer

min.insync.replicas=2:
  If kafka-3 is down, ISR = {kafka-1, kafka-2}
  acks=all still satisfied (2 ≥ min.insync.replicas)
  Write succeeds

If kafka-2 AND kafka-3 are both down:
  ISR = {kafka-1} < min.insync.replicas=2
  Write fails with NotEnoughReplicasException
  → Prevents data loss at the cost of availability
```

Durability vs. Availability tradeoff:
| Setting | Durability | Availability | Use Case |
|---|---|---|---|
| acks=0 | None | Highest | Metrics, logs (loss OK) |
| acks=1 | Leader only | High | Low-value events |
| acks=all + min.isr=1 | ISR ≥1 | Medium | Default |
| acks=all + min.isr=2 | ISR ≥2 | Lower | Orders, payments |

#### 1c. Exactly-Once Processing

```
Problem: network retry → duplicate message → duplicate DB write

Solution (3 layers):

Layer 1 — Producer idempotency (acks=all + MaxAttempts=5):
  Kafka deduplicates retried produce requests using
  (ProducerID, SequenceNumber) per partition.
  → At-least-once delivery guaranteed.

Layer 2 — Consumer idempotency (Redis SET NX):
  Before processing, check:
    SET kafka:seen:{group}:{event_id} 1 NX EX 86400
  If key already exists → skip (already processed).
  → Deduplication window = 24 hours.

Layer 3 — Manual offset commit (after processing):
  CommitMessages() called AFTER handler returns success.
  If process crashes after processing but before commit:
    → Message redelivered on restart
    → Layer 2 idempotency check catches the duplicate
  → Exactly-once semantics end-to-end.
```

#### 1d. Dead-Letter Queue

```
Processing flow:
  attempt 1 → fail → wait 100ms
  attempt 2 → fail → wait 200ms
  attempt 3 → fail → wait 400ms
  → PublishToDLQ(original_event, error)
  → Delete idempotency key (allow DLQ reprocessor to retry)
  → CommitMessages() (don't block the partition)

DLQ event schema:
  {
    "original_event": { ...full envelope... },
    "error": "connection refused",
    "failed_at": "2026-04-13T10:00:00Z"
  }

DLQ consumer group "dlq-reprocessor":
  → Logs for manual inspection
  → Alerts on-call
  → In production: retry with exponential back-off + human approval
```

#### 1e. Offset Caching in Redis

```
After each successful commit:
  SET kafka:offset:{group}:{topic}:{partition} {offset} EX 604800

On consumer restart:
  GET kafka:offset:{group}:{topic}:{partition}
  → Skip Kafka broker offset fetch (~50ms saved)
  → Kafka is still the source of truth; this is a read optimisation only

Key format: "kafka:offset:order-processor:order.created:2"
TTL: 7 days (longer than max consumer downtime)
```

#### 1f. Consumer Group Rebalancing

```
Scenario: 3 consumers, 6 partitions (order.created)

Initial assignment (RangeAssignor):
  consumer-A → partitions 0, 1
  consumer-B → partitions 2, 3
  consumer-C → partitions 4, 5

consumer-D joins:
  REBALANCE triggered (stop-the-world for all consumers)
  New assignment:
  consumer-A → partitions 0, 1
  consumer-B → partitions 2, 3
  consumer-C → partitions 4
  consumer-D → partitions 5

consumer-B crashes:
  REBALANCE triggered
  consumer-A → partitions 0, 1, 2
  consumer-C → partitions 3, 4
  consumer-D → partitions 5

Rebalance duration: SessionTimeout (30s) + RebalanceTimeout (30s)
During rebalance: no messages consumed (availability gap)
Mitigation: use CooperativeStickyAssignor (incremental rebalance) in production
```

---

### 2. Sharding — Virtual Node Consistent Hash Ring

**Files:** `internal/db/consistent_hash.go`, `internal/db/sharding.go`

```
Ring (2^32 positions, 150 virtual nodes per shard):

  0 ─── vnode(S0,0) ─── vnode(S1,0) ─── vnode(S2,0) ─── vnode(S0,1) ─── 2^32

Key "user@example.com"
  → FNV-1a hash → position P
  → clockwise successor → Shard X
```

Tradeoff: modulo vs. consistent hash ring:
| Aspect | Modulo | Consistent Hash Ring |
|---|---|---|
| Rebalancing cost | O(K) all keys | O(K/N) ~1/N of keys |
| Lookup | O(1) | O(log N×V) binary search |
| Memory | O(1) | O(N×V) ring entries |
| Hot spots | High | Low (vnodes smooth distribution) |

Cross-shard join (scatter-gather + hash join):
```
Phase 1 — Scatter:  goroutine per shard, parallel queries
Phase 2 — Build:    hash map[user_id → UserRow] in memory
Phase 3 — Probe:    for each order, O(1) lookup in hash map
Phase 4 — Sort:     merge-sort by created_at DESC

Complexity: O(U + O + O log O)
Memory:     O(U + O) — full result set in RAM
Use for:    admin dashboards, analytics (NOT hot paths)
```

---

### 3. Replication — Lag-Aware Read Routing

**File:** `internal/db/replication_monitor.go`

```
Background goroutine polls every 10s:
  SELECT EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp()))

lag < 5s  → route reads to replica (round-robin)
lag ≥ 5s  → mark degraded, fall back to primary
unreachable → mark degraded immediately
```

MongoDB: `secondaryPreferred` reads, `majority` write concern
Redis: async replication, Sentinel failover (~5-10s), reads from replicas

---

### 4. Caching Patterns

**File:** `internal/cache/advanced.go`

| Pattern | Consistency | Latency | Use Case |
|---|---|---|---|
| Cache-aside | Eventual | Low | Product listings |
| Write-through | Strong | Medium | Order creation |
| Write-behind | Eventual | Lowest | High-churn counters |
| Singleflight | Eventual | Low | Hot product pages |
| Stale-while-revalidate | Eventual | Lowest p99 | High-traffic products |
| Probabilistic TTL (XFetch) | Eventual | Low | Prevents hard expiry cliff |
| Tag-based invalidation | Eventual | Low | Bulk category invalidation |
| Event-driven invalidation | Eventual | Kafka lag | Reliable async invalidation |

---

### 5. Consistency Tradeoffs Summary

| Scenario | Pattern | Consistency | Tradeoff |
|---|---|---|---|
| Product page | Stale-while-revalidate | Eventual | Up to 30min stale stock |
| Checkout price | Strong consistency | Linearizable | Always hits DB |
| Order creation | Write-through + dist lock | Strong | 2 RTTs; duplicate prevention |
| Order events | Kafka acks=all + idempotency | Exactly-once | ~10ms extra latency |
| Cache invalidation | Event-driven (Kafka) | Eventual | Inconsistency = Kafka lag |
| Replica reads | Lag-aware routing | Eventual | Falls back to primary if lag >5s |
| Cross-shard join | Scatter-gather | Eventual | Full result set in RAM |
| DLQ events | At-least-once + manual | Best-effort | Requires human intervention |

---

### 6. Complete Event Flow Diagram

```
POST /api/v1/orders
  │
  ├─ Redis: AcquireLock (SET NX, 5s TTL)
  ├─ MongoDB: GetProduct (secondaryPreferred)
  │     └─ Redis cache-aside (singleflight)
  ├─ PostgreSQL Shard[hash(user_id)].Primary: INSERT orders + items (tx)
  ├─ MongoDB Primary: UpdateStock (write concern: majority)
  ├─ Redis Master: DEL product:{id}, DEL cart:{user}
  ├─ Redis: ReleaseLock
  └─ Kafka: Publish order.created (key=user_id, acks=all)
              │
              ├─► order-processor consumer:
              │     PostgreSQL: UPDATE orders SET status=confirmed
              │     Kafka: Publish order.confirmed
              │
              ├─► cache-invalidator consumer:
              │     Redis: DEL order:{id}
              │
              ├─► notification-svc consumer:
              │     Send order confirmation email
              │
              └─► analytics-svc consumer:
                    Write to data warehouse


GET /api/v1/products/:id?consistency=eventual
  │
  ├─ Redis Replica[round-robin]: GET product:{id}
  │     HIT → return (X-Cache: HIT, X-Cache-Strategy: singleflight-cache-aside)
  │
  └─ MISS: singleflight → MongoDB Secondary → Redis Master SET → return


GET /api/v1/admin/orders-with-users
  │
  ├─ Shard 0 Replica: SELECT orders, users ─┐
  ├─ Shard 1 Replica: SELECT orders, users ─┼─ parallel goroutines
  └─ Shard 2 Replica: SELECT orders, users ─┘
        └─ Build hash map → Probe → Sort → Return
```

---

### 7. File Reference

```
internal/
  kafka/
    topics.go      — topic names, partition counts, consumer group IDs
    events.go      — Envelope (event_id, correlation_id, schema version)
    producer.go    — partitioned producer, acks=all, Snappy, retry
    consumer.go    — consumer group, Redis idempotency, DLQ, offset cache
    admin.go       — EnsureTopics (idempotent startup)
    handlers.go    — OrderProcessor, StockProcessor, CacheInvalidator,
                     NotificationHandler, AnalyticsHandler, DLQReprocessor
  db/
    sharding.go           — ShardManager, consistent hash ring
    consistent_hash.go    — virtual-node ring (150 vnodes/shard)
    replication_monitor.go— lag polling, degraded replica detection
    cross_shard_join.go   — scatter-gather, application-level hash join
    migrations.go         — schema applied to every shard
  cache/
    redis.go     — master/replica client, rate limit, distributed lock
    advanced.go  — write-through, write-behind, singleflight, XFetch, SWR,
                   tag invalidation, inconsistency tracking
  handlers/
    auth_handler.go    — register/login + publish user.registered
    product_handler.go — cache strategies per ?consistency param
    order_handler.go   — create order + publish order.created
    cart_handler.go    — Redis-only cart
    admin_handler.go   — shard stats, lag, cross-shard join, cache audit
  middleware/
    auth.go       — JWT validation
    ratelimit.go  — sliding-window rate limit via Redis pipeline
cmd/
  api/main.go    — wires all components, starts HTTP server
  worker/main.go — starts all Kafka consumer groups + replication monitor
```


---

## 5. Redis — Sharding, Replication, and Tradeoffs

### 5a. Two Deployment Modes

```
Mode A: Standalone + Sentinel (dev default, cluster.enabled=false)
  redis-master ──► redis-replica1
               └──► redis-replica2
  redis-sentinel1 monitors master, promotes replica on failure

Mode B: Redis Cluster (production, cluster.enabled=true)
  redis-cluster-1 (master, slots 0-5460)     ← redis-cluster-4 (replica)
  redis-cluster-2 (master, slots 5461-10922) ← redis-cluster-5 (replica)
  redis-cluster-3 (master, slots 10923-16383)← redis-cluster-6 (replica)
```

### 5b. Redis Cluster Sharding (Partitioning)

```
16384 hash slots distributed across 3 masters:

Key routing:
  CRC16("{product:123}:data") % 16384 → slot 8432 → redis-cluster-2

Hash tags co-locate related keys on the same slot:
  {user:abc}:cart    → CRC16("user:abc") % 16384 → same slot
  {user:abc}:session → CRC16("user:abc") % 16384 → same slot
  → MGET, pipelines, Lua scripts work without CROSSSLOT error

Without hash tags:
  product:123:data  → slot X (any master)
  product:123:lock  → slot Y (different master)
  → MGET fails with CROSSSLOT error
```

Slot distribution (3 masters, 16384 slots):
| Master | Slot Range | Keys (uniform) |
|---|---|---|
| redis-cluster-1 | 0 – 5460 | ~33% |
| redis-cluster-2 | 5461 – 10922 | ~33% |
| redis-cluster-3 | 10923 – 16383 | ~33% |

### 5c. Redis Cluster Replication

```
Each master has 1 replica on a different physical node:
  redis-cluster-1 (master) → redis-cluster-4 (replica)

Replication is asynchronous:
  Write to master → master acks → replicates to replica
  Inconsistency window: replication lag (typically <1ms on LAN)

Automatic failover (no Sentinel needed):
  If redis-cluster-1 fails:
    Cluster detects failure (cluster-node-timeout=5000ms)
    redis-cluster-4 promoted to master
    Slot range 0-5460 now served by redis-cluster-4
    Total failover time: ~5-10s
```

### 5d. MOVED and ASK Redirects (Resharding)

```
Normal operation:
  Client → redis-cluster-2 (GET product:123)
  redis-cluster-2 owns slot 8432 → serves request

During resharding (slot migration):
  Slot 8432 being moved from cluster-2 to cluster-3
  Client → redis-cluster-2 (GET product:123)
  redis-cluster-2: "ASK 8432 redis-cluster-3"
  Client → redis-cluster-3 (ASKING + GET product:123)
  → Transparent to application (MaxRedirects=8)

After resharding:
  Client → redis-cluster-2 (GET product:123)
  redis-cluster-2: "MOVED 8432 redis-cluster-3"
  Client updates slot map, retries on redis-cluster-3
  → Transparent, but causes brief latency spike
```

### 5e. Hot-Key Problem and Solutions

```
Problem:
  All requests for "product:viral-item" → same slot → same master
  1M req/s on one node while others are idle

Solution 1: Local Shard Cache (in-process)
  HotKeyDetector tracks access frequency per key.
  When key exceeds threshold (e.g., 1000 req/window):
    → Promote to LocalShardCache (in-process, 5s TTL)
    → Subsequent reads: zero Redis RTT
  Tradeoff: stale data up to 5s; memory pressure on app process;
            each API instance has independent local cache

Solution 2: Fan-Out Writes (key replication across slots)
  Write: SET {product:123}:data:shard:0, :shard:1, :shard:2
  Read:  GET {product:123}:data:shard:{random(0,2)}
  → Load spread across 3 different slots/nodes
  Tradeoff: 3× write amplification; invalidation must touch all copies

Solution 3: Read from Replicas (ReadFromReplica=true)
  Reads routed to replica nodes → master handles only writes
  Tradeoff: stale reads (replication lag, typically <1ms)
```

### 5f. Distributed Lock Tradeoffs

```
Single-node lock (AcquireLock):
  SET lock:{key} {uuid} NX PX {ttl_ms}
  Fast (1 RTT), but unsafe during master failover:
    Master holds lock → master fails → replica promoted
    New master doesn't know about the lock
    → Two clients may hold the lock simultaneously

Redlock (RedlockAcquire):
  Acquire lock on majority of masters (2/3)
  Safe during failover: even if one master fails,
  the other two still hold the lock
  Tradeoff: 3 RTTs (one per master) vs. 1 for single-node
  Use for: order creation, payment processing

Recommendation:
  Use single-node lock for low-value operations (cart updates)
  Use Redlock for high-value operations (orders, payments)
```

### 5g. Standalone vs. Cluster Comparison

| Aspect | Standalone + Sentinel | Redis Cluster |
|---|---|---|
| Write scaling | Single master (bottleneck) | N masters (linear) |
| Memory scaling | Single machine | N machines (linear) |
| Failover time | 5-30s (Sentinel) | 5-10s (built-in) |
| Multi-key ops | Any keys | Same hash slot only |
| Lua scripts | Any keys | Same hash slot only |
| SELECT (DB) | 0-15 | DB 0 only |
| Resharding | N/A | Brief latency spike |
| Complexity | Low | Medium |
| Use case | Dev, small scale | Production, large scale |

### 5h. New Admin Endpoints

```
GET  /api/v1/admin/redis-cluster    — per-node memory, keyspace, replication info
GET  /api/v1/admin/redis-hot-keys   — top-10 hot keys + local cache promotion status
GET  /api/v1/admin/redis-slot?key=X — which slot and node owns a given key
```

### 5i. New Files

```
internal/cache/
  cluster.go   — ClusterClient: topology-aware routing, Redlock, hash tags,
                 slot info, per-node stats, CRC16 slot calculator
  hotkey.go    — HotKeyDetector (sliding window), LocalShardCache (in-process LRU),
                 HotKeyShardedClient (fan-out writes, local cache promotion)
  unified.go   — CacheClient interface + NewCacheClient factory (mode selection)
```
