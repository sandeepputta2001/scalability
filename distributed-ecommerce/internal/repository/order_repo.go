package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/consistency"
	"github.com/distributed-ecommerce/internal/db"
	"github.com/distributed-ecommerce/internal/models"
	"github.com/distributed-ecommerce/internal/outbox"
)

// OrderRepository stores orders in sharded PostgreSQL.
// All reads are consistency-aware: the pool selected (primary vs. replica)
// depends on the consistency.Level in the context.
type OrderRepository struct {
	sm      *db.ShardManager
	monitor *db.ReplicationMonitor
	log     *zap.Logger
}

func NewOrderRepository(sm *db.ShardManager, monitor *db.ReplicationMonitor, log *zap.Logger) *OrderRepository {
	return &OrderRepository{sm: sm, monitor: monitor, log: log}
}

// Create inserts an order, its items, AND an outbox event in a single atomic
// transaction. This is the Transactional Outbox Pattern.
//
// outboxPayload is the pre-serialised kafka.Envelope JSON — built by the caller
// to avoid an import cycle between repository and kafka packages.
func (r *OrderRepository) Create(ctx context.Context, order *models.Order, outboxEvent *outbox.Event) error {
	shard := r.sm.ShardForKey(order.UserID.String())
	order.ShardKey = shard.ID
	order.ID = uuid.New()
	order.CreatedAt = time.Now()
	order.UpdatedAt = time.Now()

	tx, err := WritePool(shard).Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// 1. Insert the order
	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, user_id, status, total_price, shard_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		order.ID, order.UserID, order.Status, order.TotalPrice,
		order.ShardKey, order.CreatedAt, order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	// 2. Insert order items
	for i := range order.Items {
		order.Items[i].ID = uuid.New()
		order.Items[i].OrderID = order.ID
		_, err = tx.Exec(ctx, `
			INSERT INTO order_items (id, order_id, product_id, quantity, unit_price)
			VALUES ($1, $2, $3, $4, $5)`,
			order.Items[i].ID, order.Items[i].OrderID,
			order.Items[i].ProductID, order.Items[i].Quantity, order.Items[i].UnitPrice,
		)
		if err != nil {
			return fmt.Errorf("insert order item: %w", err)
		}
	}

	// 3. Insert outbox event IN THE SAME TRANSACTION
	outboxID := uuid.New()
	now := time.Now()
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, event_type, payload, topic, partition_key,
			 status, attempts, created_at, next_retry_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',0,$8,$8)`,
		outboxID,
		outboxEvent.AggregateType,
		order.ID.String(), // use the newly assigned order ID
		outboxEvent.EventType,
		outboxEvent.Payload,
		outboxEvent.Topic,
		outboxEvent.PartitionKey,
		now,
	)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}

	// 4. Commit — order + outbox event committed atomically
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Record write for Session consistency
	if req := consistency.FromContext(ctx); req.SessionToken != "" {
		consistency.RecordWrite(req.SessionToken)
	}

	r.log.Debug("order + outbox event created atomically",
		zap.String("order_id", order.ID.String()),
		zap.String("outbox_id", outboxID.String()),
		zap.Int("shard", shard.ID))
	return nil
}

// GetOutboxRepo returns an outbox.Repository for the shard that owns the given key.
func (r *OrderRepository) GetOutboxRepo(key string, log *zap.Logger) *outbox.Repository {
	shard := r.sm.ShardForKey(key)
	return outbox.NewRepository(WritePool(shard), log)
}

// GetByID fetches an order. Pool selection is consistency-aware.
func (r *OrderRepository) GetByID(ctx context.Context, orderID uuid.UUID, userID uuid.UUID) (*models.Order, error) {
	shard := r.sm.ShardForKey(userID.String())
	pool := ReadPool(ctx, shard, r.monitor)

	row := pool.QueryRow(ctx, `
		SELECT id, user_id, status, total_price, shard_key, created_at, updated_at
		FROM orders WHERE id = $1 AND user_id = $2`, orderID, userID)

	var o models.Order
	if err := row.Scan(&o.ID, &o.UserID, &o.Status, &o.TotalPrice,
		&o.ShardKey, &o.CreatedAt, &o.UpdatedAt); err != nil {
		return nil, fmt.Errorf("get order: %w", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT id, order_id, product_id, quantity, unit_price
		FROM order_items WHERE order_id = $1`, orderID)
	if err != nil {
		return nil, fmt.Errorf("get order items: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var item models.OrderItem
		if err := rows.Scan(&item.ID, &item.OrderID, &item.ProductID,
			&item.Quantity, &item.UnitPrice); err != nil {
			return nil, err
		}
		o.Items = append(o.Items, item)
	}
	return &o, nil
}

// ListByUser returns all orders for a user. Pool selection is consistency-aware.
func (r *OrderRepository) ListByUser(ctx context.Context, userID uuid.UUID) ([]*models.Order, error) {
	shard := r.sm.ShardForKey(userID.String())
	pool := ReadPool(ctx, shard, r.monitor)

	rows, err := pool.Query(ctx, `
		SELECT id, user_id, status, total_price, shard_key, created_at, updated_at
		FROM orders WHERE user_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		return nil, fmt.Errorf("list orders: %w", err)
	}
	defer rows.Close()

	var orders []*models.Order
	for rows.Next() {
		var o models.Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Status, &o.TotalPrice,
			&o.ShardKey, &o.CreatedAt, &o.UpdatedAt); err != nil {
			return nil, err
		}
		orders = append(orders, &o)
	}
	return orders, nil
}

// UpdateStatus updates order status — always writes to primary.
func (r *OrderRepository) UpdateStatus(ctx context.Context, orderID uuid.UUID, userID uuid.UUID, status models.OrderStatus) error {
	shard := r.sm.ShardForKey(userID.String())
	_, err := WritePool(shard).Exec(ctx, `
		UPDATE orders SET status = $1, updated_at = $2
		WHERE id = $3 AND user_id = $4`,
		status, time.Now(), orderID, userID,
	)
	if err == nil {
		if req := consistency.FromContext(ctx); req.SessionToken != "" {
			consistency.RecordWrite(req.SessionToken)
		}
	}
	return err
}

// ScatterGatherOrders queries ALL shards in parallel and merges results.
// Uses replica pools for eventual consistency (admin/analytics use).
func (r *OrderRepository) ScatterGatherOrders(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error) {
	type result struct {
		orders []*models.Order
		err    error
	}
	ch := make(chan result, len(r.sm.AllShards()))

	for _, shard := range r.sm.AllShards() {
		go func(s *db.Shard) {
			// Scatter-gather always uses replicas (eventual consistency)
			pool := r.monitor.ShardByID(s.ID).ReplicaPool()
			rows, err := pool.Query(ctx, `
				SELECT id, user_id, status, total_price, shard_key, created_at, updated_at
				FROM orders WHERE status = $1 ORDER BY created_at DESC LIMIT $2`,
				status, limit)
			if err != nil {
				ch <- result{err: err}
				return
			}
			defer rows.Close()

			var orders []*models.Order
			for rows.Next() {
				var o models.Order
				if err := rows.Scan(&o.ID, &o.UserID, &o.Status, &o.TotalPrice,
					&o.ShardKey, &o.CreatedAt, &o.UpdatedAt); err != nil {
					ch <- result{err: err}
					return
				}
				orders = append(orders, &o)
			}
			ch <- result{orders: orders}
		}(shard)
	}

	var all []*models.Order
	for range r.sm.AllShards() {
		res := <-ch
		if res.err != nil {
			r.log.Warn("scatter-gather shard error", zap.Error(res.err))
			continue
		}
		all = append(all, res.orders...)
	}
	return all, nil
}
