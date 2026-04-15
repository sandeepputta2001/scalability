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


---

## 6. Load Balancing

**File:** `internal/loadbalancer/loadbalancer.go`

### 6a. Algorithms

```
Round-Robin:
  requests: 1→2→3→1→2→3
  state:    atomic counter (lock-free)
  tradeoff: ignores node capacity and latency

Weighted Round-Robin (Nginx smooth algorithm):
  node A weight=2, node B weight=1:
  round 1: A.current=2, B.current=1 → pick A → A.current=2-3=-1
  round 2: A.current=1, B.current=2 → pick B → B.current=2-3=-1
  round 3: A.current=3, B.current=0 → pick A → A.current=3-3=0
  result: A,B,A — smooth, not bursty
  tradeoff: requires manual weight tuning; doesn't adapt to runtime load

Least Connections:
  pick node with min(active_conns)
  active_conns tracked with atomic.Int64 per node
  IncrConns() before request, DecrConns() after (defer)
  tradeoff: thundering herd if slow node suddenly frees up

Consistent Hash (virtual nodes):
  same key → same node (session affinity, cache locality)
  150 virtual nodes per real node → even distribution
  tradeoff: more memory; O(log N) lookup vs O(1) round-robin
```

### 6b. Health Checking

```
Background goroutine probes every 10s:
  probe(ctx, node) → error? → mark unhealthy
  no error?        → mark healthy, reset fail count

All algorithms skip unhealthy nodes.
If all nodes unhealthy → ErrNoHealthyNodes → caller handles fallback.

Exponential back-off: fail count tracked per node.
In production: use fail count to implement back-off before re-admitting.
```

### 6c. Where Used

| Component | Algorithm | Reason |
|---|---|---|
| PostgreSQL replicas | Weighted Round-Robin | Equal weights in dev; tune for prod |
| MongoDB replica set | Least Connections | Variable query duration |
| Redis replicas | Round-Robin | Already in cache.Client |
| HTTP upstreams | Consistent Hash | Session affinity |

---

## 7. Data Partitioning

**File:** `internal/partition/partitioner.go`

### 7a. Hash Partitioning

```
key → FNV-1a(key) % N → partition

Example (N=3):
  "user@a.com" → hash=1234567 → 1234567 % 3 = 1 → partition 1
  "user@b.com" → hash=9876543 → 9876543 % 3 = 0 → partition 0

Tradeoff:
  ✓ Uniform distribution (on average)
  ✓ O(1) routing
  ✗ Adding partition N+1 remaps ~N/(N+1) of all keys
  ✗ No range queries across partitions
  ✗ Hot spots if data is skewed (e.g., one user has 90% of orders)
```

### 7b. Range Partitioning

```
boundaries = ["M", "Z"]  → 3 partitions

key < "M"  → partition 0  (A-L)
key < "Z"  → partition 1  (M-Y)
key >= "Z" → partition 2  (Z+)

Binary search: O(log B) where B = number of boundaries

Tradeoff:
  ✓ Range queries within a partition (e.g., all products A-M)
  ✓ Easy to reason about data locality
  ✗ Hot spots if data is skewed (e.g., most products start with "A")
  ✗ Manual boundary tuning required
  ✗ Rebalancing requires moving data between partitions
```

### 7c. Directory Partitioning

```
lookup table: {"user-vip-1": 0, "user-vip-2": 0, ...}
unregistered keys → fall back to hash partitioning

Migrate("user-vip-1", 2):
  lookup["user-vip-1"] = 2
  → next request for "user-vip-1" goes to partition 2

Tradeoff:
  ✓ Maximum flexibility (move any key to any partition)
  ✓ Supports VIP/tenant isolation
  ✗ Lookup table = single point of failure
  ✗ Lookup table = bottleneck at high QPS
  ✗ Must keep lookup table consistent (use Redis or distributed KV)
```

### 7d. Composite Partitioning

```
key = "user-123:order-456"
  level 1: hash("user-123") % 3 = 1  → shard group 1
  level 2: range("order-456") = 0    → sub-partition 0
  global partition = 1 * 3 + 0 = 3

All data for "user-123" is in shard group 1.
Within shard group 1, orders are range-partitioned.

Tradeoff:
  ✓ Tenant isolation (all tenant data on same shard group)
  ✓ Range queries within a tenant
  ✗ Cross-tenant queries require scatter-gather
  ✗ More complex routing logic
```

### 7e. Distribution Analyzer

```
Tracks key counts per partition.
Computes: mean, stddev, skew = stddev/mean

skew = 0.0  → perfectly uniform
skew = 0.2  → 20% variation → rebalance_recommended = true
skew = 1.0  → one partition has all the data (severe hot spot)

Hottest partition: most keys → candidate for splitting
Coldest partition: fewest keys → candidate for merging
```

---

## 8. Consistency Levels

**Files:** `internal/consistency/consistency.go`, `internal/middleware/consistency.go`, `internal/repository/consistency_router.go`

### 8a. Decision Tree

```
Request arrives with X-Consistency: strong

ConsistencyMiddleware:
  ParseLevel("strong") → Strong
  WithConsistency(ctx, Request{Level: Strong})

Repository.GetByID(ctx, ...):
  RouteRead(ctx, shardID, getLag)
    → Strong → ReadFromPrimary
  pool = shard.Primary
  pool.QueryRow(...)

Response headers:
  X-Consistency-Level: strong
  X-Data-Source: primary
```

### 8b. Session Consistency (Read-Your-Writes)

```
POST /api/v1/orders  (X-Session-Token: sess-abc)
  → orderRepo.Create(ctx, order)
  → consistency.RecordWrite("sess-abc")  // timestamp stored

GET /api/v1/orders   (X-Session-Token: sess-abc, X-Consistency: session)
  → ConsistencyMiddleware:
      LastWrite("sess-abc") = 2s ago
      Request{Level: Session, WriteTimestamp: 2s ago}
  → RouteRead:
      time.Since(WriteTimestamp) = 2s < 5s → ReadFromPrimary
  → reads from primary (guaranteed to see the just-created order)

GET /api/v1/orders   (X-Session-Token: sess-abc, X-Consistency: session)
  (6 seconds later)
  → RouteRead:
      time.Since(WriteTimestamp) = 6s > 5s → ReadFromReplica
  → reads from replica (replica has caught up by now)
```

### 8c. Bounded Staleness

```
GET /api/v1/products/:id
  (X-Consistency: bounded-staleness, X-Max-Staleness-Ms: 10000)

  → RouteRead:
      getLag(shardID, 0) = 3.2s
      3.2s < 10s → ReadFromReplica  ✓

  → RouteRead (if replica lag = 15s):
      15s > 10s → ReadFromPrimary  (replica too stale)
```

### 8d. CAP Theorem Mapping

```
Network partition scenario:
  Primary is reachable, replica is not (or vice versa)

Strong consistency (CP):
  → Always read from primary
  → If primary unreachable: request fails (consistency > availability)
  → PostgreSQL primary-only reads

Eventual consistency (AP):
  → Read from replica
  → If replica unreachable: fall back to primary (availability > consistency)
  → MongoDB secondaryPreferred, Redis replica reads

Bounded staleness (between CP and AP):
  → Read from replica if lag < bound
  → Fall back to primary if replica too stale
  → Tunable: tighter bound → more CP-like; looser bound → more AP-like
```

### 8e. Consistency per Data Type

| Data | Default Level | Rationale |
|---|---|---|
| Product listing | Eventual | Stale stock count OK for browsing |
| Product detail (checkout) | Strong | Must show accurate price/stock |
| Order creation | Strong | Idempotency + accurate stock check |
| Order history | Session | Read-your-writes after placing order |
| Cart | Eventual | Redis-only; no DB replication |
| User profile | Session | See own updates immediately |
| Analytics | Eventual | Stale data acceptable for dashboards |

---

## 9. New Files Summary

```
internal/
  loadbalancer/
    loadbalancer.go  — RoundRobin, WeightedRoundRobin, LeastConnections,
                       ConsistentHash, HealthChecker, LoadBalancer.Do()

  partition/
    partitioner.go   — HashPartitioner, RangePartitioner, DirectoryPartitioner,
                       CompositePartitioner, DistributionAnalyzer, Router

  consistency/
    consistency.go   — Level enum, Request, WithConsistency/FromContext,
                       RouteRead, WriteConcernFor, SessionStore, ValidateRead,
                       ResponseHeaders

  middleware/
    consistency.go   — ConsistencyMiddleware (injects level into context)

  repository/
    consistency_router.go — ReadPool/WritePool (consistency-aware pool selection)
    order_repo.go    — updated: uses ReadPool/WritePool, records session writes

  handlers/
    lb_partition_handler.go — GetLBStats, GetPartitionReport, RouteKey
```


---

## 10. Retry Mechanisms

**File:** `internal/retry/retry.go`

### 10a. Exponential Backoff with Jitter

```
attempt 1 fails → wait = 100ms × 2^0 × (1 + rand(0, 0.5)) = 100-150ms
attempt 2 fails → wait = 100ms × 2^1 × (1 + rand(0, 0.5)) = 200-300ms
attempt 3 fails → wait = 100ms × 2^2 × (1 + rand(0, 0.5)) = 400-600ms
...capped at MaxDelay

Without jitter (thundering herd):
  t=0:    1000 clients fail simultaneously
  t=100ms: 1000 clients retry simultaneously → server overloaded again
  t=200ms: 1000 clients retry simultaneously → server overloaded again

With jitter (spread):
  t=0:    1000 clients fail simultaneously
  t=100-150ms: ~333 clients retry (spread across 50ms window)
  t=200-300ms: ~333 clients retry (spread across 100ms window)
  t=400-600ms: ~333 clients retry (spread across 200ms window)
  → Server load spread across time → recovery possible
```

### 10b. Circuit Breaker State Machine

```
                    failures >= maxFailures
    ┌─────────┐ ─────────────────────────────► ┌──────────┐
    │  CLOSED │                                 │   OPEN   │
    │(normal) │ ◄─────────────────────────────  │(fast-fail│
    └─────────┘    probe success                └──────────┘
                                                      │
                                          resetTimeout elapsed
                                                      │
                                                      ▼
                                               ┌────────────┐
                                               │ HALF-OPEN  │
                                               │(one probe) │
                                               └────────────┘
                                                      │
                                          probe fails │
                                                      ▼
                                               back to OPEN

State transitions:
  Closed → Open:      failures.Load() >= maxFailures
  Open → Half-Open:   time.Since(lastFail) >= resetTimeout
  Half-Open → Closed: probe succeeds
  Half-Open → Open:   probe fails

Tradeoff:
  While Open: valid requests are rejected (availability ↓)
  Benefit: downstream service gets time to recover (no overload)
  Without CB: every request hits the failing service → cascading failure
```

### 10c. Retry Budget

```
Budget(50): max 50 concurrent retries across all goroutines

Scenario without budget:
  1000 goroutines, each retries 5 times = 5000 total calls
  → Amplifies load by 5× during an outage

Scenario with budget(50):
  1000 goroutines attempt retry
  50 acquire budget tokens → retry
  950 fail fast (budget exhausted)
  → Load amplification capped at 50 extra calls
  → System has headroom to recover

Token lifecycle:
  Acquire() before retry → CAS decrement
  Release() after attempt (success or failure) → CAS increment
  If budget.remaining == 0 → fail fast, return immediately
```

### 10d. Error Classification

```
IsTransient(err):
  false: context.Canceled, context.DeadlineExceeded
  false: duplicate key, permission denied, syntax error (DB)
  true:  connection reset, timeout, temporary failure

Why not retry context.Canceled?
  The client cancelled the request. Retrying would waste resources
  on a response nobody is waiting for.

Why not retry duplicate key?
  The operation already succeeded (idempotency). Retrying would
  create a second duplicate, not fix the error.
```

---

## 11. Transactional Outbox Pattern

**Files:** `internal/outbox/outbox.go`, `internal/repository/order_repo.go`, `internal/handlers/order_handler.go`

### 11a. The Dual-Write Problem

```
Without outbox (broken):
  ┌─────────────────────────────────────────────────────┐
  │ HTTP Handler                                        │
  │   1. INSERT order → PostgreSQL  ✓                  │
  │   2. CRASH / network timeout                        │
  │   3. Publish → Kafka            ✗ never happens     │
  └─────────────────────────────────────────────────────┘
  Result: order in DB, no Kafka event, downstream services
          never know the order was placed.
```

### 11b. Outbox Solution

```
With outbox (correct):
  ┌─────────────────────────────────────────────────────┐
  │ HTTP Handler                                        │
  │   1. Build outbox_event payload (JSON envelope)     │
  │   2. Pass to orderRepo.Create(ctx, order, outboxEvt)│
  └─────────────────────────────────────────────────────┘
           │
           ▼
  ┌─────────────────────────────────────────────────────┐
  │ PostgreSQL Transaction (single shard)               │
  │   BEGIN                                             │
  │   INSERT INTO orders ...          ← business entity │
  │   INSERT INTO order_items ...                       │
  │   INSERT INTO outbox_events ...   ← event record    │
  │   COMMIT  ← atomic: all or nothing                  │
  └─────────────────────────────────────────────────────┘
           │
           ▼ (async, poll every 1s)
  ┌─────────────────────────────────────────────────────┐
  │ Outbox Relay Worker (per shard)                     │
  │   SELECT ... FOR UPDATE SKIP LOCKED                 │
  │   WHERE status IN ('pending','failed')              │
  │   AND next_retry_at <= NOW()                        │
  │                                                     │
  │   For each event:                                   │
  │     retry.DoWithBudget → producer.PublishRaw        │
  │     success → UPDATE status='published'             │
  │     failure → UPDATE attempts++, next_retry_at=...  │
  └─────────────────────────────────────────────────────┘
```

### 11c. Outbox Event Lifecycle

```
INSERT (status='pending', attempts=0, next_retry_at=NOW())
  │
  ▼ relay picks up
PublishRaw to Kafka
  │
  ├─ success → UPDATE status='published', published_at=NOW()
  │
  └─ failure → UPDATE attempts++, next_retry_at=NOW()+backoff
                 attempts=1 → next_retry_at = NOW()+1s
                 attempts=2 → next_retry_at = NOW()+2s
                 attempts=3 → next_retry_at = NOW()+4s
                 ...
                 attempts=10 → UPDATE status='failed'
                               (requires manual intervention)
```

### 11d. SELECT FOR UPDATE SKIP LOCKED

```
Multiple relay workers can run concurrently (one per shard, or scaled out):

Worker A:                          Worker B:
  SELECT ... FOR UPDATE SKIP LOCKED  SELECT ... FOR UPDATE SKIP LOCKED
  → locks rows [1,2,3]               → locks rows [4,5,6] (skips 1,2,3)
  → processes [1,2,3]                → processes [4,5,6]

Without SKIP LOCKED:
  Worker B would block waiting for Worker A to release locks.
  → No parallelism, relay becomes a bottleneck.

With SKIP LOCKED:
  Worker B immediately gets the next available rows.
  → Linear scaling: N workers = N× throughput.
```

### 11e. At-Least-Once Delivery and Idempotency

```
Failure scenario:
  1. Relay publishes event to Kafka  ✓
  2. Relay crashes before UPDATE status='published'
  3. On restart: relay sees event still 'pending'
  4. Relay publishes event to Kafka AGAIN (duplicate)

Consumer handles duplicate:
  SET kafka:seen:{group}:{event_id} 1 NX EX 86400
  → If key exists: skip (already processed)
  → If key doesn't exist: process and set key

Result: exactly-once processing despite at-least-once delivery.
```

### 11f. Direct Publish + Outbox (Dual Path)

```
Order creation flow:
  1. Atomic DB write (order + outbox event)
  2. Try direct Kafka publish (circuit-breaker protected, async goroutine)
     → If Kafka is healthy: event delivered in <1ms
     → If Kafka is down: circuit breaker opens, skip direct publish
  3. Outbox relay delivers event within poll interval (default 1s)

This gives:
  - Low latency when Kafka is healthy (direct publish wins)
  - Guaranteed delivery when Kafka is down (outbox relay wins)
  - No duplicate processing (consumer idempotency handles both paths)
```

### 11g. Outbox Table Schema

```sql
CREATE TABLE outbox_events (
    id             UUID        PRIMARY KEY,
    aggregate_type TEXT        NOT NULL,   -- "order", "user"
    aggregate_id   TEXT        NOT NULL,   -- order_id
    event_type     TEXT        NOT NULL,   -- "order.created"
    payload        JSONB       NOT NULL,   -- full kafka.Envelope JSON
    topic          TEXT        NOT NULL,   -- "order.created"
    partition_key  TEXT        NOT NULL,   -- user_id (for Kafka partitioning)
    status         TEXT        NOT NULL DEFAULT 'pending',
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    next_retry_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for relay polling (only pending/failed events, ordered by creation)
CREATE INDEX idx_outbox_status_retry
    ON outbox_events(status, next_retry_at)
    WHERE status IN ('pending', 'failed');
```

---

## 12. New Files Summary

```
internal/
  retry/
    retry.go     — Do, DoWithLog, DoWithBudget (exponential backoff + jitter)
                   CircuitBreaker (Closed/Open/Half-Open state machine)
                   Budget (retry storm prevention)
                   IsTransient, IsTransientDB, IsTransientHTTP

  outbox/
    outbox.go    — Event model, Repository (Insert, FetchPending, MarkPublished,
                   MarkFailed, GetStats), Publisher interface,
                   Relay (poll loop, SELECT FOR UPDATE SKIP LOCKED,
                   retry with budget, exponential backoff on failure)

Updated:
  db/migrations.go         — outbox_events table + indexes added to every shard
  repository/order_repo.go — Create() now writes order + outbox event atomically
  handlers/order_handler.go — builds outbox event, uses retry for MongoDB calls,
                              circuit breaker on direct Kafka publish
  cmd/worker/main.go       — starts one outbox relay per shard
  handlers/admin_handler.go — /admin/outbox-stats endpoint
```


---

## 13. Design Patterns — New Additions

**File:** `internal/patterns/`

### 13a. Builder Pattern

```
Problem (telescoping constructor):
  func NewOrder(userID, item1, qty1, price1, item2, qty2, price2, status string) *Order
  → 8 parameters, easy to mix up, no validation until runtime

Solution (Builder):
  order, err := NewOrderBuilder(userID).
    AddItem("prod-1", 2, 99.99).
    AddItem("prod-2", 1, 49.99).
    WithStatus("pending").
    Build()  ← validates all fields here, returns error if invalid

Rules enforced at Build():
  - userID must not be nil
  - at least one item required
  - quantity > 0, price >= 0
  - status must be a valid value

QueryBuilder:
  Assembles parameterised SQL WHERE clauses.
  Prevents SQL injection by always using $N placeholders.
  Composes conditions with AND automatically.
```

### 13b. Observer / Event Bus

```
In-process pub/sub (complement to Kafka for cross-service events):

  eventBus.Subscribe("product.updated", func(ctx, event) error {
    return redisClient.InvalidateProduct(ctx, event.ProductID)
  })

  // In product handler after stock update:
  eventBus.Publish(ctx, ProductUpdatedEvent{ProductID: id, NewStock: 45})
  // → cache invalidation fires asynchronously, handler doesn't import cache

Async mode (default):
  Publish() returns immediately.
  Handlers run in goroutines.
  Panics are recovered and logged.

Sync mode:
  Publish() waits for all handlers.
  Returns first error.
  Use for: audit logging, validation hooks.

Tradeoff vs. Kafka:
  ✓ Zero latency (no network, no serialisation)
  ✗ Not durable (lost on process crash)
  ✗ Not distributed (only within one process)
```

### 13c. CQRS

```
Write path (Commands):
  CreateOrderCommand → validate → CommandBus.Dispatch → handler → DB write
  UpdateOrderStatusCommand → validate → handler → DB write + event

Read path (Queries):
  GetOrderQuery → QueryBus.Execute → handler → replica read → return view model
  ListOrdersByUserQuery → handler → replica read → return list

Read models (Projections):
  OrderReadModel: denormalised, includes user email/name (pre-joined)
  ProductReadModel: includes computed fields (in_stock, stock_level)

  These are updated by Kafka consumers when events arrive:
    order.created → update OrderReadModel
    stock.updated → update ProductReadModel.stock, recompute stock_level

Tradeoff:
  ✓ Read replicas serve queries without touching write primary
  ✓ Read models optimised for specific query shapes
  ✗ Eventual consistency between write and read models
  ✗ More code: separate command/query handlers
```

### 13d. Saga Orchestrator

```
Order Placement Saga (4 steps):

  Step 1: ReserveStock (MongoDB)
    Compensate: ReleaseStock

  Step 2: CreateOrder (PostgreSQL shard)
    Compensate: CancelOrder

  Step 3: ChargePayment (external service)
    Compensate: RefundPayment

  Step 4: ConfirmOrder (PostgreSQL shard)
    Compensate: CancelOrder

Failure at Step 3 (ChargePayment):
  → Compensate Step 2: CancelOrder
  → Compensate Step 1: ReleaseStock
  → Saga status: "failed" (cleanly rolled back)

Failure at Step 3 AND compensation of Step 2 also fails:
  → Saga status: "comp_failed" (needs manual intervention)
  → Alert on-call, write to incident tracker

Tradeoff vs. 2PC:
  ✓ No distributed locks (each step commits locally)
  ✓ Works across heterogeneous databases
  ✗ Intermediate states visible (e.g., stock reserved but order not yet created)
  ✗ Compensations must be idempotent
```

### 13e. Bulkhead

```
Resource pools per operation type:

  product-reads:  200 concurrent, 100ms timeout
  order-writes:   50 concurrent,  500ms timeout
  payment-calls:  20 concurrent,  2s timeout
  cache-reads:    500 concurrent, 50ms timeout

Failure isolation scenario:
  Payment service is slow → fills payment-calls bulkhead (20 slots)
  New payment requests: rejected immediately (ErrBulkheadFull)
  Product reads: unaffected (separate 200-slot bulkhead)
  Order writes: unaffected (separate 50-slot bulkhead)

Without bulkheads:
  Payment slowness fills the shared goroutine pool
  → All operations start timing out
  → Full system outage from one slow dependency

Semaphore implementation:
  sem := make(chan struct{}, maxConcurrent)
  Acquire: sem <- struct{}{}  (blocks or times out)
  Release: <-sem              (always in defer)
```

### 13f. Decorator

```
Composable cross-cutting concerns on ProductReader:

  var reader ProductReader = productRepo          // base
  reader = WithLogging(reader, log)               // add logging
  reader = WithBulkhead(reader, productBulkhead)  // add concurrency limit
  reader = WithRetry(reader, DBConfig, log)       // add retry

All three decorators implement ProductReader.
The handler only sees ProductReader — unaware of decorators.

Execution order for GetByID:
  RetryDecorator.GetByID
    → BulkheadDecorator.GetByID (acquire slot)
      → LoggingDecorator.GetByID (start timer)
        → ProductRepository.GetByID (actual DB call)
      → LoggingDecorator (log duration + error)
    → BulkheadDecorator (release slot)
  → RetryDecorator (retry if transient error)

Tradeoff:
  ✓ Each concern is independently testable
  ✓ Add/remove concerns without changing repository
  ✗ Stack depth increases with each decorator
  ✗ Interface must be kept narrow (every method needs a wrapper)
```

### 13g. Specification

```
Composable query predicates:

  // Business rule: available electronics under $500 with "laptop" tag
  spec := InCategory("electronics").
    And(PriceBelow(500)).
    And(HasTag("laptop")).
    And(InStock())

  // Same spec generates both MongoDB and SQL queries:
  mongoFilter := spec.ToMongoFilter()
  // → {"$and": [{"category":"electronics"}, {"price":{"$lt":500}},
  //             {"tags":"laptop"}, {"stock":{"$gt":0}}]}

  sqlClause, args := spec.ToSQLWhere()
  // → "(category = $1 AND price < $2 AND $3 = ANY(tags) AND stock > 0)"
  // → args: ["electronics", 500, "laptop"]

  // In-memory filtering:
  spec.IsSatisfiedBy(product)  // true/false

Tradeoff:
  ✓ Business rules expressed in domain language
  ✓ Reusable: same spec used for MongoDB, SQL, and in-memory filtering
  ✗ Complex specs may generate inefficient queries (no index hints)
  ✗ Leaky abstraction: DB-specific optimisations are harder
```

---

## 14. Complete Pattern Inventory

| # | Pattern | Category | File |
|---|---|---|---|
| 1 | Repository | Data Access | `repository/*.go` |
| 2 | Consistent Hash Ring | Distributed | `db/consistent_hash.go` |
| 3 | Scatter-Gather | Distributed | `db/cross_shard_join.go` |
| 4 | Cache-Aside | Caching | `cache/advanced.go` |
| 5 | Write-Through | Caching | `cache/advanced.go` |
| 6 | Write-Behind | Caching | `cache/advanced.go` |
| 7 | Singleflight | Concurrency | `cache/advanced.go` |
| 8 | Stale-While-Revalidate | Caching | `cache/advanced.go` |
| 9 | Distributed Lock | Concurrency | `cache/redis.go` |
| 10 | Redlock | Concurrency | `cache/cluster.go` |
| 11 | Rate Limiting | Resilience | `middleware/ratelimit.go` |
| 12 | Circuit Breaker | Resilience | `retry/retry.go` |
| 13 | Retry + Backoff | Resilience | `retry/retry.go` |
| 14 | Retry Budget | Resilience | `retry/retry.go` |
| 15 | Transactional Outbox | Messaging | `outbox/outbox.go` |
| 16 | Event Envelope | Messaging | `kafka/events.go` |
| 17 | Consumer Group | Messaging | `kafka/consumer.go` |
| 18 | Dead-Letter Queue | Messaging | `kafka/consumer.go` |
| 19 | Load Balancer (4 algos) | Distributed | `loadbalancer/loadbalancer.go` |
| 20 | Data Partitioning (4 strategies) | Distributed | `partition/partitioner.go` |
| 21 | Consistency Levels | Distributed | `consistency/consistency.go` |
| 22 | Hot-Key Detection | Caching | `cache/hotkey.go` |
| 23 | Builder | Creational | `patterns/builder.go` |
| 24 | Observer / Event Bus | Behavioural | `patterns/eventbus.go` |
| 25 | CQRS | Architectural | `patterns/cqrs.go` |
| 26 | Saga Orchestrator | Distributed | `patterns/saga.go` |
| 27 | Bulkhead | Resilience | `patterns/bulkhead.go` |
| 28 | Decorator | Structural | `patterns/decorator.go` |
| 29 | Specification | Behavioural | `patterns/specification.go` |
