package db

import (
	"context"
	"fmt"

	"go.uber.org/zap"
)

// schema applied to every shard
const schema = `
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    name          TEXT NOT NULL,
    shard_key     INT  NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id     UUID NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    total_price NUMERIC(12,2) NOT NULL,
    shard_key   INT  NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id   UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id TEXT NOT NULL,
    quantity   INT  NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_user_id   ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status    ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON order_items(order_id);
`

// RunMigrations applies the schema to every shard's primary.
func RunMigrations(ctx context.Context, sm *ShardManager, log *zap.Logger) error {
	fullSchema := schema + "\n" + outboxSchema
	for _, shard := range sm.AllShards() {
		if _, err := shard.Primary.Exec(ctx, fullSchema); err != nil {
			return fmt.Errorf("migration on shard %d: %w", shard.ID, err)
		}
		log.Info("migrations applied", zap.Int("shard", shard.ID))
	}
	return nil
}

// outboxSchema is the DDL for the transactional outbox table.
const outboxSchema = `
CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    topic          TEXT        NOT NULL,
    partition_key  TEXT        NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'pending',
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    next_retry_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_retry
    ON outbox_events(status, next_retry_at)
    WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox_events(aggregate_type, aggregate_id);
`
