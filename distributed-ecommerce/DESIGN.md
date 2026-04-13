# Distributed E-Commerce — HLD & LLD

---

## High-Level Design (HLD)

### System Overview

```
                        ┌──────────────────────────────────────────────────────┐
                        │                   Clients                            │
                        │         (Web / Mobile / Admin Dashboard)             │
                        └──────────────────────┬───────────────────────────────┘
                                               │ HTTPS
                        ┌──────────────────────▼───────────────────────────────┐
                        │              API Gateway / Load Balancer             │
                        │         (rate limiting, TLS termination)             │
                        └──────────────────────┬───────────────────────────────┘
                                               │
                        ┌──────────────────────▼───────────────────────────────┐
                        │                 API Server (Go/Gin)                  │
                        │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐  │
                        │  │  Auth    │ │ Products │ │  Orders  │ │  Cart  │  │
                        │  │ Handler  │ │ Handler  │ │ Handler  │ │Handler │  │
                        │  └──────────┘ └──────────┘ └──────────┘ └────────┘  │
                        │  ┌──────────────────────────────────────────────────┐ │
                        │  │              Middleware Layer                    │ │
                        │  │  JWT Auth │ Rate Limit (Redis) │ Recovery        │ │
                        │  └──────────────────────────────────────────────────┘ │
                        └──────┬──────────────┬──────────────┬──────────────────┘
                               │              │              │
               ┌───────────────▼──┐  ┌────────▼──────┐  ┌───▼──────────────────┐
               │  PostgreSQL       │  │   MongoDB      │  │       Redis          │
               │  (Sharded)        │  │  (Replica Set) │  │  (Master/Replica)    │
               │                   │  │                │  │                      │
               │  Shard 0          │  │  Primary       │  │  Master (writes)     │
               │  ├─ Primary        │  │  Secondary ×2  │  │  Replica ×2 (reads) │
               │  └─ Replica(s)    │  │                │  │  Sentinel ×1 (HA)   │
               │                   │  │  readPref:     │  │                      │
               │  Shard 1          │  │  secondary     │  │  Patterns:           │
               │  ├─ Primary        │  │  Preferred     │  │  • Cache-aside       │
               │  └─ Replica(s)    │  │                │  │  • Write-through     │
               │                   │  │  Write concern:│  │  • Write-behind      │
               │  Shard 2          │  │  majority      │  │  • Stale-while-rev.  │
               │  ├─ Primary        │  │                │  │  • Singleflight      │
               │  └─ Replica(s)    │  │                │  │  • Probabilistic TTL │
               └───────────────────┘  └────────────────┘  └──────────────────────┘
```

### Data Partitioning Strategy

| Data Type    | Storage         | Sharding Key  | Rationale                                      |
|--------------|-----------------|---------------|------------------------------------------------|
| Users        | PostgreSQL      | email (hash)  | Uniform distribution; login always knows email |
| Orders       | PostgreSQL      | user_id (hash)| All orders for a user on one shard = fast list |
| Products     | MongoDB         | None (replica)| Catalog is read-heavy; replication > sharding  |
| Carts        | Redis           | user_id       | Ephemeral; TTL-based; no durability needed     |
| Sessions     | Redis           | token         | Ephemeral; fast lookup by token                |

---

## Low-Level Design (LLD)

### 1. Sharding — Virtual Node Consistent Hash Ring

**File:** `internal/db/consistent_hash.go`, `internal/db/sharding.go`

```
Ring (2^32 positions):

  0 ──── vnode(S0,0) ──── vnode(S1,0) ──── vnode(S2,0) ──── vnode(S0,1) ──── 2^32
                │                │                │
              Shard 0          Shard 1          Shard 2

Key "user@example.com"
  → FNV-1a hash → position P on ring
  → clockwise successor → Shard X
```

**Why virtual nodes (150 per shard)?**
- Plain modulo: adding shard 3 remaps ~75% of keys
- Virtual nodes: adding shard 3 remaps only ~25% of keys
- 150 vnodes gives <5% standard deviation in key distribution

**Tradeoff table:**

| Aspect              | Modulo Hashing     | Consistent Hash Ring        |
|---------------------|--------------------|-----------------------------|
| Rebalancing cost    | O(K) — all keys    | O(K/N) — 1/N of keys        |
| Lookup complexity   | O(1)               | O(log N*V) binary search    |
| Memory              | O(1)               | O(N*V) ring entries         |
| Hot spot risk       | High (uneven dist) | Low (vnodes smooth it out)  |

**Cross-Shard Join Pattern** (`internal/db/cross_shard_join.go`):

```
SQL JOIN (impossible across shards):
  SELECT o.*, u.email FROM orders o JOIN users u ON o.user_id = u.id
  ✗ orders and users may be on different PostgreSQL instances

Application-Level Hash Join (implemented):
  Phase 1 — Scatter:  goroutine per shard, parallel queries
  Phase 2 — Build:    hash map[user_id → UserRow] in memory
  Phase 3 — Probe:    for each order, O(1) lookup in hash map
  Phase 4 — Sort:     merge-sort by created_at DESC

Complexity: O(U + O + O log O)  where U=users, O=orders
Memory:     O(U + O) — entire result set in RAM
```

**When to use scatter-gather:**
- Admin dashboards, analytics, reporting (low frequency)
- NOT for hot user-facing paths (use shard-local queries)

---

### 2. Replication — Lag-Aware Read Routing

**File:** `internal/db/replication_monitor.go`

```
Background goroutine polls every 10s:
  SELECT EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp()))

Decision tree per read request:
  ┌─ replica.lag < 5s? ──YES──► route read to replica (round-robin)
  │
  └─ NO (lagged/down) ─────────► fall back to primary

State transitions:
  healthy ──(lag > 5s)──► degraded ──(lag < 5s)──► healthy
                                   ──(unreachable)──► degraded
```

**Replication Lag Tradeoff:**

| Read Mode          | Consistency    | Latency  | Primary Load | Use Case                    |
|--------------------|----------------|----------|--------------|-----------------------------|
| Replica (default)  | Eventual       | Low      | Low          | Product listings, search    |
| Primary fallback   | Strong         | Medium   | High         | Checkout, payment confirm   |
| Strong (forced)    | Linearizable   | High     | Very High    | Inventory deduction         |

**MongoDB Replication:**
- Write concern: `majority` — write acknowledged by primary + 1 secondary before returning
- Read preference: `secondaryPreferred` — reads go to secondaries, fall back to primary
- Inconsistency window: up to replication lag (typically <100ms in LAN)

**Redis Replication:**
- Async replication: master acknowledges write before replica confirms
- Sentinel monitors master; promotes replica on failure (failover ~5-10s)
- During failover: writes fail; reads from replicas may be stale

---

### 3. Caching — All Patterns

**File:** `internal/cache/redis.go`, `internal/cache/advanced.go`

#### 3a. Cache-Aside (Lazy Loading)
```
Read:
  1. Check Redis → HIT: return cached value
  2. MISS: query DB → store in Redis → return value

Write:
  Update DB → invalidate cache key

Inconsistency window: between DB write and cache invalidation
  (typically <1ms on same host, up to seconds under load)
```

#### 3b. Write-Through
```
Write:
  1. Write to DB (source of truth)
  2. Write to cache (same value, same TTL)
  → Cache always consistent with DB after write

Tradeoff: +1 network RTT per write; cache may hold unread data
```

#### 3c. Write-Behind (Write-Back)
```
Write:
  1. Write to cache immediately (fast path, <1ms)
  2. Enqueue DB write to buffered channel (async)
  3. Background goroutine drains channel → DB write

DATA LOSS RISK: if process crashes before drain, DB write is lost
Mitigation: retry up to 3x; fall back to sync write if buffer full

Tradeoff: lowest write latency vs. durability risk
```

#### 3d. Singleflight (Thundering Herd Prevention)
```
Problem: 1000 goroutines all miss cache simultaneously → 1000 DB queries

Solution:
  goroutine 1: cache miss → becomes "leader", queries DB
  goroutines 2-999: cache miss → wait on leader's WaitGroup
  leader returns → all 999 waiters get the same result

Result: 1 DB query instead of 1000
```

#### 3e. Probabilistic Early Expiry (XFetch)
```
Problem: hard TTL expiry → all goroutines miss at the same moment

Solution (XFetch algorithm):
  P(recompute) = -β × ln(remaining_ttl / original_ttl)

  As TTL approaches 0, probability of early recompute increases.
  One goroutine recomputes before expiry → cache never goes cold.

β=1.0 means: at 50% remaining TTL, ~30% chance of early recompute
```

#### 3f. Stale-While-Revalidate
```
Two TTLs per key:
  freshKey (5 min):  serve without revalidation
  staleKey (30 min): serve while revalidating in background

Request flow:
  freshKey HIT  → return immediately (X-Cache: FRESH)
  freshKey MISS + staleKey HIT → return stale + async refresh (X-Cache: STALE)
  both MISS → synchronous DB query + populate both keys

p99 latency: always cache read latency (never DB latency)
Tradeoff: clients may see data up to 30 min old
```

#### 3g. Tag-Based Invalidation
```
Problem: invalidating all products in a category requires knowing all keys

Solution:
  key format: "product:v{version}:{id}"
  version stored in Redis: "tag:products" → 42

  Invalidate all products:
    INCR "tag:products" → 43
    All keys with v42 are now "invisible" (version mismatch)
    Old keys expire naturally via TTL

Tradeoff: old keys accumulate in Redis until TTL expires (memory waste)
```

#### 3h. Distributed Lock (Redlock-lite)
```
Acquire: SET lock:{key} {uuid} NX PX {ttl_ms}
  NX = only set if not exists
  PX = auto-expire (prevents deadlock if holder crashes)

Release: Lua script (atomic check-and-delete)
  if GET lock:{key} == uuid then DEL lock:{key}
  (prevents releasing another holder's lock)

Used for: preventing duplicate order submissions within 5s window
```

---

### 4. Consistency Tradeoffs Summary

| Scenario                        | Pattern Used              | Consistency Level | Tradeoff                              |
|---------------------------------|---------------------------|-------------------|---------------------------------------|
| Product page load               | Stale-while-revalidate    | Eventual          | May show 30min old stock count        |
| Checkout product price          | Strong consistency        | Linearizable      | Always hits DB; higher latency        |
| Cart operations                 | Redis-only (no DB)        | None (ephemeral)  | Cart lost if Redis fails              |
| Order creation                  | Write-through + dist lock | Strong            | Duplicate prevention; 2 RTTs          |
| Replication lag > 5s            | Fall back to primary      | Strong            | Higher primary load during lag spike  |
| Cache/DB inconsistency detected | Auto-heal + audit log     | Eventual → Strong | Inconsistency window logged           |
| Cross-shard join                | Scatter-gather + hash join| Eventual          | Full result set in RAM; slow for large|

---

### 5. Component Interaction Diagram

```
POST /api/v1/orders
  │
  ├─► Redis: AcquireLock("order-lock:{user_id}", uuid, 5s)
  │     └─ if lock exists → 409 Conflict
  │
  ├─► MongoDB: GetByID(product_id)  [secondaryPreferred read]
  │     └─ cache miss → replica → populate Redis cache
  │
  ├─► PostgreSQL Shard[hash(user_id)].Primary: INSERT orders + order_items (tx)
  │
  ├─► MongoDB Primary: UpdateStock(product_id, -qty)  [write concern: majority]
  │
  ├─► Redis Master: DEL "product:{id}"  [cache invalidation]
  │
  ├─► Redis Master: DEL "cart:{user_id}"
  │
  └─► Redis: ReleaseLock("order-lock:{user_id}", uuid)


GET /api/v1/products/:id?consistency=eventual
  │
  ├─► Redis Replica[round-robin]: GET "product:{id}"
  │     └─ HIT → return (X-Cache: HIT)
  │
  └─► MISS: singleflight leader queries MongoDB Secondary
        └─ populate Redis Master → return (X-Cache: MISS)


GET /api/v1/admin/orders-with-users
  │
  ├─► Shard 0 Replica: SELECT orders, users  ─┐
  ├─► Shard 1 Replica: SELECT orders, users  ─┼─ parallel goroutines
  └─► Shard 2 Replica: SELECT orders, users  ─┘
        │
        └─► Gather → Build hash map[user_id] → Probe orders → Sort → Return
```

---

### 6. File Structure Reference

```
internal/
  db/
    sharding.go           — ShardManager, virtual-node consistent hash ring
    consistent_hash.go    — ConsistentHashRing implementation (150 vnodes/shard)
    replication_monitor.go— Lag polling, degraded replica detection, fallback
    cross_shard_join.go   — Scatter-gather, application-level hash join
    migrations.go         — Schema applied to every shard

  cache/
    redis.go              — Client, round-robin replicas, rate limit, dist lock
    advanced.go           — Write-through, write-behind, singleflight,
                            probabilistic TTL, stale-while-revalidate,
                            tag invalidation, inconsistency tracking

  repository/
    user_repo.go          — Shard routing by email hash
    order_repo.go         — Shard routing by user_id, scatter-gather
    product_repo.go       — MongoDB CRUD, text search, stock update

  handlers/
    auth_handler.go       — Register/login, JWT generation
    product_handler.go    — Cache strategies selectable per request
    order_handler.go      — Distributed lock, write-through, stock deduction
    cart_handler.go       — Redis-only cart (ephemeral)
    admin_handler.go      — Shard stats, lag report, cross-shard join, audit log

  middleware/
    auth.go               — JWT validation, claims injection
    ratelimit.go          — Sliding-window rate limit via Redis pipeline
```
