# API Reference — Distributed E-Commerce

Base URL: `http://localhost:8080`
OpenAPI spec: `docs/openapi.yaml` (import into Postman, Insomnia, or Swagger UI)

---

## Global Headers

Every request can carry these headers:

| Header | Values | Effect |
|---|---|---|
| `Authorization` | `Bearer <jwt>` | Required for protected endpoints |
| `X-Consistency` | `eventual` `session` `bounded-staleness` `strong` | Consistency level for DB reads |
| `X-Session-Token` | any string | Enables read-your-writes tracking for `session` consistency |
| `X-Max-Staleness-Ms` | integer ms | Max allowed replica lag for `bounded-staleness` |
| `X-Correlation-ID` | UUID | Propagated to Kafka events for distributed tracing |

Every response includes:

| Header | Meaning |
|---|---|
| `X-Consistency-Level` | Consistency level that was applied |
| `X-Data-Source` | `primary` or `replica` |
| `X-Data-Age-Ms` | Age of data in ms (replica reads only) |
| `X-RateLimit-Limit` | 100 |
| `X-RateLimit-Remaining` | Remaining requests in current minute |

---

## Authentication

### POST /api/v1/auth/register

Register a new user. Stores the user in the PostgreSQL shard determined by
consistent-hashing the email. Publishes `user.registered` to Kafka.

**Request**
```json
{
  "email": "alice@example.com",
  "password": "s3cr3tpass",
  "name": "Alice Smith"
}
```

**Response `201`**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "user": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "email": "alice@example.com",
    "name": "Alice Smith",
    "shard_key": 1,
    "created_at": "2026-04-15T10:00:00Z"
  }
}
```

**Errors**
- `400` — missing/invalid fields
- `409` — email already registered

---

### POST /api/v1/auth/login

Authenticate and receive a JWT (valid 24 hours).

**Request**
```json
{ "email": "alice@example.com", "password": "s3cr3tpass" }
```

**Response `200`** — same shape as register response.

**Errors**
- `400` — missing fields
- `401` — invalid credentials

---

## Products

### GET /api/v1/products

List products with optional category filter and pagination.

**Query params**

| Param | Default | Description |
|---|---|---|
| `category` | — | Exact category match |
| `page` | `1` | Page number |
| `page_size` | `20` | Items per page (max 100) |
| `consistency` | `eventual` | Read consistency level |

**Response `200`**
```json
{
  "products": [ { ...Product } ],
  "page": 1,
  "page_size": 20
}
```

---

### GET /api/v1/products/search?q=laptop

Full-text search across product name and description (MongoDB `$text` index).
Returns up to 20 results.

**Response `200`**
```json
{ "products": [ { ...Product } ] }
```

**Errors**
- `400` — missing `q` parameter

---

### GET /api/v1/products/:id

Fetch a single product. Caching strategy is controlled by `consistency`:

| `consistency` | Strategy | Cache behaviour | Response headers |
|---|---|---|---|
| `eventual` (default) | Singleflight cache-aside | Redis → miss → MongoDB → populate | `X-Cache-Strategy: singleflight-cache-aside` |
| `stale` | Stale-while-revalidate | Serve stale, refresh async | `X-Cache: STALE\|FRESH`, `Warning: 110` |
| `strong` | Always DB | Skip cache, read primary, heal cache | `X-Cache-Strategy: strong-consistency` |

**Response `200`** — Product object.

**Errors**
- `404` — product not found

---

### POST /api/v1/products *(auth required)*

Create a product. Uses write-through caching (DB write + cache populate atomically).

**Request**
```json
{
  "name": "MacBook Pro",
  "description": "Apple M3 laptop",
  "price": 2499.99,
  "stock": 50,
  "category": "electronics",
  "tags": ["laptop", "apple"],
  "attributes": { "color": "silver", "ram": "16GB" }
}
```

**Response `201`** — created Product with assigned `id`.

---

## Orders *(auth required)*

### GET /api/v1/orders

List all orders for the authenticated user. Single-shard query (no scatter-gather).

Use `X-Consistency: session` immediately after placing an order to guarantee
the new order is visible (read-your-writes).

**Response `200`**
```json
{ "orders": [ { ...Order } ] }
```

---

### POST /api/v1/orders

Place a new order. Uses the **Transactional Outbox Pattern** — the order and
its Kafka event are written atomically. Kafka delivery is guaranteed even if
the broker is temporarily down.

**Request**
```json
{
  "items": [
    { "product_id": "64a1f2b3c4d5e6f7a8b9c0d1", "quantity": 2 },
    { "product_id": "64a1f2b3c4d5e6f7a8b9c0d2", "quantity": 1 }
  ]
}
```

**Response `201`** — created Order with status `pending`.

**Errors**
- `400` — product not found or insufficient stock
- `409` — duplicate submission within 5s window
- `500` — DB write failed

**Side effects**
- Decrements stock in MongoDB
- Invalidates product Redis cache
- Publishes `order.created` to Kafka (via outbox relay if direct publish fails)
- Clears the user's cart

---

### GET /api/v1/orders/:id

Fetch a single order with its line items. Only the order owner can access it.

**Errors**
- `400` — invalid UUID format
- `404` — not found or not owned by this user

---

## Cart *(auth required)*

Cart is stored entirely in Redis (TTL 30 minutes). Not persisted to PostgreSQL.

### GET /api/v1/cart

Returns the current cart. Returns an empty cart if none exists.

**Response `200`**
```json
{
  "user_id": "550e8400-...",
  "items": [
    { "product_id": "64a1...", "name": "MacBook Pro", "quantity": 1, "unit_price": 2499.99 }
  ],
  "updated_at": "2026-04-15T10:05:00Z"
}
```

---

### POST /api/v1/cart/items

Add an item to the cart. If the product is already in the cart, quantity is incremented.

**Request**
```json
{ "product_id": "64a1f2b3c4d5e6f7a8b9c0d1", "quantity": 1 }
```

**Response `200`** — updated Cart.

**Errors**
- `404` — product not found in MongoDB

---

### DELETE /api/v1/cart/items/:product_id

Remove a specific item from the cart.

**Response `200`** — updated Cart (item removed).

---

### DELETE /api/v1/cart

Clear the entire cart.

**Response `200`**
```json
{ "message": "cart cleared" }
```

---

## Admin Endpoints *(auth required)*

All admin endpoints require a valid JWT. In production, add an admin role check.

---

### Sharding

#### GET /api/v1/admin/shards

Per-shard row counts and consistent hash ring virtual-node distribution.

```json
{
  "shards": [
    { "shard_id": 0, "user_count": 3421, "order_count": 8903 },
    { "shard_id": 1, "user_count": 3398, "order_count": 8812 }
  ],
  "ring_distribution": { "0": 150, "1": 150, "2": 150 },
  "total_shards": 3
}
```

#### GET /api/v1/admin/orders-with-users?status=pending&limit=50

Cross-shard scatter-gather + application-level hash join. Fans out to all
PostgreSQL shards in parallel, joins orders with users in memory.

| Param | Default | Description |
|---|---|---|
| `status` | — | Filter by order status |
| `limit` | `50` | Max results (capped at 200) |

---

### Replication

#### GET /api/v1/admin/replication-lag

PostgreSQL replication lag per shard/replica. Replicas with `lag_seconds > 5`
are marked `degraded=true` and skipped for reads.

```json
{
  "shards": [
    {
      "shard_id": 0,
      "replicas": [{ "index": 0, "lag_seconds": 0.12, "degraded": false }]
    }
  ],
  "max_lag_threshold": 5.0
}
```

---

### Cache

#### GET /api/v1/admin/cache-inconsistencies

Last 100 detected cache/DB inconsistencies (stock or price mismatch between
Redis and MongoDB, detected during `strong` consistency reads).

#### GET /api/v1/admin/cache-tag/:tag

Current version number for a named cache tag.

#### POST /api/v1/admin/cache-tag/:tag/bump

Increment the tag version, effectively invalidating all tagged cache keys.

#### GET /api/v1/admin/redis-cluster

Per-node memory and keyspace stats (Redis Cluster mode only).

#### GET /api/v1/admin/redis-hot-keys

Top-10 most accessed Redis keys. Hot keys are promoted to local in-process cache.

#### GET /api/v1/admin/redis-slot?key=product:123

Compute which Redis Cluster slot owns a given key (`CRC16(key) % 16384`).

---

### Kafka / Outbox

#### GET /api/v1/admin/outbox-stats

Per-shard outbox event counts by status.

```json
{
  "shards": [
    { "shard_id": 0, "counts": { "pending": 2, "published": 15043, "failed": 0 } }
  ]
}
```

| Status | Meaning |
|---|---|
| `pending` | Awaiting relay publication |
| `published` | Delivered to Kafka |
| `failed` | Max retries exceeded — needs manual fix |

---

### Load Balancer & Partitioning

#### GET /api/v1/admin/lb-stats

Per-node health, active connections, and algorithm for DB replica LB
(weighted round-robin) and MongoDB LB (least-connections).

#### GET /api/v1/admin/partition-report

Data distribution skew across partitions. `rebalance_recommended=true` when
skew coefficient > 0.2.

#### GET /api/v1/admin/route-key?key=alice@example.com&entity=user

Show which partition a key routes to under the entity's partitioning strategy.

| `entity` | Strategy | Shard key |
|---|---|---|
| `order` | Hash | user_id |
| `user` | Hash | email |
| `product` | Range | category |

---

### Design Patterns

#### GET /api/v1/admin/bulkheads

Per-bulkhead capacity, in-use count, and rejection count.

| Bulkhead | Capacity | Timeout |
|---|---|---|
| product-reads | 200 | 100ms |
| order-writes | 50 | 500ms |
| payment-calls | 20 | 2s |
| cache-reads | 500 | 50ms |

#### GET /api/v1/admin/circuit-breakers

State (`closed`/`open`/`half-open`) and failure count for all circuit breakers.

#### POST /api/v1/admin/run-saga

Run a demo order placement saga. Set `fail_at` to simulate failure and observe compensation.

**Request**
```json
{ "user_id": "550e8400-e29b-41d4-a716-446655440000", "fail_at": "ChargePayment" }
```

**Response** — full `SagaExecution` trace with per-step status and compensation results.

#### GET /api/v1/admin/spec-demo?category=electronics&tag=laptop

Build a composite Specification and see the equivalent MongoDB filter and SQL WHERE clause.

#### POST /api/v1/admin/cqrs-demo

Demonstrate CQRS: validate a Command and a Query side by side.

#### POST /api/v1/admin/builder-demo

Demonstrate the Builder pattern: valid Order construction and invalid Product validation.

---

## Error Format

All errors use a consistent JSON envelope:

```json
{ "error": "human-readable message" }
```

## HTTP Status Codes

| Code | Meaning |
|---|---|
| `200` | Success |
| `201` | Created |
| `400` | Bad request / validation error |
| `401` | Missing or invalid JWT |
| `404` | Resource not found |
| `409` | Conflict (duplicate, lock held) |
| `429` | Rate limit exceeded |
| `500` | Internal server error |

## Data Models

### Product
```json
{
  "id": "64a1f2b3c4d5e6f7a8b9c0d1",
  "name": "MacBook Pro",
  "description": "Apple M3 laptop",
  "price": 2499.99,
  "stock": 48,
  "category": "electronics",
  "tags": ["laptop", "apple"],
  "attributes": { "color": "silver" },
  "created_at": "2026-04-01T09:00:00Z",
  "updated_at": "2026-04-15T10:05:00Z"
}
```

### Order
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440001",
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "total_price": 5049.97,
  "shard_key": 1,
  "created_at": "2026-04-15T10:10:00Z",
  "updated_at": "2026-04-15T10:10:00Z",
  "items": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440002",
      "order_id": "550e8400-e29b-41d4-a716-446655440001",
      "product_id": "64a1f2b3c4d5e6f7a8b9c0d1",
      "quantity": 2,
      "unit_price": 2499.99
    }
  ]
}
```

### Cart
```json
{
  "user_id": "550e8400-e29b-41d4-a716-446655440000",
  "items": [
    { "product_id": "64a1...", "name": "MacBook Pro", "quantity": 1, "unit_price": 2499.99 }
  ],
  "updated_at": "2026-04-15T10:05:00Z"
}
```
