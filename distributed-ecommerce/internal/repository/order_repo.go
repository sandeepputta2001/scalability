package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/db"
	"github.com/distributed-ecommerce/internal/models"
)

// OrderRepository stores orders in sharded PostgreSQL.
// Sharding key: user_id — all orders for a user live on the same shard,
// making per-user queries fast without scatter-gather.
type OrderRepository struct {
	sm  *db.ShardManager
	log *zap.Logger
}

func NewOrderRepository(sm *db.ShardManager, log *zap.Logger) *OrderRepository {
	return &OrderRepository{sm: sm, log: log}
}

// Create inserts an order and its items in a single transaction on the correct shard.
func (r *OrderRepository) Create(ctx context.Context, order *models.Order) error {
	shard := r.sm.ShardForKey(order.UserID.String())
	order.ShardKey = shard.ID
	order.ID = uuid.New()
	order.CreatedAt = time.Now()
	order.UpdatedAt = time.Now()

	tx, err := shard.Primary.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, user_id, status, total_price, shard_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		order.ID, order.UserID, order.Status, order.TotalPrice,
		order.ShardKey, order.CreatedAt, order.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

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

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	r.log.Debug("order created", zap.String("id", order.ID.String()), zap.Int("shard", shard.ID))
	return nil
}

// GetByID fetches an order with its items from the correct shard.
func (r *OrderRepository) GetByID(ctx context.Context, orderID uuid.UUID, userID uuid.UUID) (*models.Order, error) {
	shard := r.sm.ShardForKey(userID.String())
	pool := shard.ReplicaPool()

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

// ListByUser returns all orders for a user — single-shard query.
func (r *OrderRepository) ListByUser(ctx context.Context, userID uuid.UUID) ([]*models.Order, error) {
	shard := r.sm.ShardForKey(userID.String())
	rows, err := shard.ReplicaPool().Query(ctx, `
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

// UpdateStatus updates order status — write goes to primary.
func (r *OrderRepository) UpdateStatus(ctx context.Context, orderID uuid.UUID, userID uuid.UUID, status models.OrderStatus) error {
	shard := r.sm.ShardForKey(userID.String())
	_, err := shard.Primary.Exec(ctx, `
		UPDATE orders SET status = $1, updated_at = $2
		WHERE id = $3 AND user_id = $4`,
		status, time.Now(), orderID, userID,
	)
	return err
}

// ScatterGatherOrders queries ALL shards and merges results (admin use).
func (r *OrderRepository) ScatterGatherOrders(ctx context.Context, status models.OrderStatus, limit int) ([]*models.Order, error) {
	type result struct {
		orders []*models.Order
		err    error
	}
	ch := make(chan result, len(r.sm.AllShards()))

	for _, shard := range r.sm.AllShards() {
		go func(s *db.Shard) {
			rows, err := s.ReplicaPool().Query(ctx, `
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
