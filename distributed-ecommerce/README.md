# Distributed E-Commerce Platform

A production-grade Go service demonstrating **sharding**, **replication**, and **caching** across PostgreSQL, MongoDB, and Redis.

---

## Architecture

```
Client
  │
  ▼
┌─────────────────────────────────────────────────────┐
│  API Server (Gin)                                   │
│  • Rate limiting (Redis sliding window)             │
│  • JWT auth                                         │
│  • Cache-aside (Redis → MongoDB/PostgreSQL)         │
└──────┬──────────────┬──────────────┬────────────────┘
       │              │              │
       ▼              ▼              ▼
┌────────────┐  ┌──────────────┐  ┌──────────────────┐
│ PostgreSQL │  │   MongoDB    │  │      Redis        │
│  Sharding  │  │ Replica Set  │  │  Master/Replica   │
│            │  │              │  │  + Sentinel HA    │
│ Shard 0    │  │ Primary      │  │                   │
│  primary   │  │ Secondary ×2 │  │ Master (writes)   │
│  replicas  │  │              │  │ Replica ×2 (reads)│
│            │  │ readPref:    │  │ Sentinel ×1       │
│ Shard 1    │  │ secondary    │  │                   │
│ Shard 2    │  │ Preferred    │  │                   │
└────────────┘  └──────────────┘  └──────────────────┘
```

## Key Concepts Demonstrated

### Sharding (PostgreSQL)
- **Consistent hashing** (FNV-1a mod N) routes users and orders to the correct shard
- Shard key stored on every record — no scatter-gather needed for user-scoped queries
- **Scatter-gather** implemented for admin cross-shard queries (parallel goroutines)
- `ShardManager` abstracts all routing; handlers never touch shard logic directly

### Replication (PostgreSQL + MongoDB + Redis)
| Layer | Strategy |
|---|---|
| PostgreSQL | 3 independent shard nodes (each acts as primary for its key range) |
| MongoDB | 3-node replica set (1 primary + 2 secondaries), `secondaryPreferred` reads |
| Redis | 1 master + 2 replicas + 1 Sentinel for automatic failover |

- Writes always go to the **primary/master**
- Reads are **round-robin load balanced** across replicas
- Redis Sentinel monitors master health and promotes a replica on failure

### Caching (Redis)
- **Cache-aside** pattern for product reads (check Redis → miss → MongoDB → populate cache)
- **TTL-based expiry**: products 5 min, carts 30 min, sessions 1 hr
- **Write-through invalidation**: product cache cleared on stock update
- **Shopping cart** stored entirely in Redis (ephemeral, high-churn data)
- **Distributed lock** (SET NX PX) prevents duplicate order submissions
- **Sliding-window rate limiting** via Redis INCR + EXPIRE pipeline

---

## Quick Start

```bash
# Start all infrastructure + API
make up

# Check health
make health

# Seed sample data
make seed

# View logs
make logs
```

## API Endpoints

```
POST   /api/v1/auth/register
POST   /api/v1/auth/login

GET    /api/v1/products?category=electronics&page=1
GET    /api/v1/products/search?q=laptop
GET    /api/v1/products/:id
POST   /api/v1/products          (auth required)

GET    /api/v1/cart              (auth required)
POST   /api/v1/cart/items        (auth required)
DELETE /api/v1/cart/items/:id    (auth required)
DELETE /api/v1/cart              (auth required)

GET    /api/v1/orders            (auth required)
POST   /api/v1/orders            (auth required)
GET    /api/v1/orders/:id        (auth required)

GET    /health
```

## Stack

| Component | Role |
|---|---|
| Go + Gin | API server |
| PostgreSQL ×3 | Sharded order/user storage |
| MongoDB ×3 | Replicated product catalog |
| Redis ×3 + Sentinel | Caching, sessions, rate limiting, distributed locks |
| Docker Compose | Local orchestration |
