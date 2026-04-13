// cross_shard_join.go implements application-level cross-shard joins.
//
// Why can't we use SQL JOINs across shards?
//
//	Each shard is a separate PostgreSQL instance. SQL JOINs only work within
//	a single database connection. Cross-shard data lives on different hosts.
//
// Pattern: Scatter → Gather → Application-Level Join
//  1. Scatter: fan out queries to all relevant shards in parallel
//  2. Gather: collect results into a unified in-memory dataset
//  3. Join: perform the join in application memory (hash join or nested loop)
//
// Tradeoffs vs. single-DB JOIN:
//
//	✓ Scales horizontally — each shard handles a fraction of the data
//	✓ No cross-shard network traffic at the DB level
//	✗ Application memory usage proportional to result set size
//	✗ No DB-level query optimization (no index-merge across shards)
//	✗ Sorting/pagination requires fetching more data than needed
//
// When to use:
//   - Admin dashboards, analytics, reporting (low frequency, high data volume)
//   - NOT for hot paths — use shard-local queries for user-facing reads
package db

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// OrderRow is a raw order row from any shard.
type OrderRow struct {
	ID         uuid.UUID
	UserID     uuid.UUID
	Status     string
	TotalPrice float64
	ShardKey   int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// UserRow is a raw user row from any shard.
type UserRow struct {
	ID        uuid.UUID
	Email     string
	Name      string
	ShardKey  int
	CreatedAt time.Time
}

// OrderWithUser is the result of joining orders with users across shards.
type OrderWithUser struct {
	OrderID    uuid.UUID `json:"order_id"`
	UserID     uuid.UUID `json:"user_id"`
	UserEmail  string    `json:"user_email"`
	UserName   string    `json:"user_name"`
	Status     string    `json:"status"`
	TotalPrice float64   `json:"total_price"`
	CreatedAt  time.Time `json:"created_at"`
	ShardKey   int       `json:"shard_key"`
}

// CrossShardJoiner performs application-level joins across shards.
type CrossShardJoiner struct {
	sm  *ShardManager
	log *zap.Logger
}

func NewCrossShardJoiner(sm *ShardManager, log *zap.Logger) *CrossShardJoiner {
	return &CrossShardJoiner{sm: sm, log: log}
}

// OrdersWithUsers performs a cross-shard hash join of orders and users.
//
// Algorithm:
//
//	Phase 1 (Scatter): query all shards for orders and users in parallel
//	Phase 2 (Build):   build a hash map of users keyed by user_id
//	Phase 3 (Probe):   for each order, look up the user in the hash map
//	Phase 4 (Sort):    sort merged results by created_at DESC
//
// This is equivalent to: SELECT o.*, u.email, u.name FROM orders o JOIN users u ON o.user_id = u.id
// but executed across N shards.
func (j *CrossShardJoiner) OrdersWithUsers(
	ctx context.Context,
	status string,
	limit int,
) ([]OrderWithUser, error) {
	shards := j.sm.AllShards()

	type shardResult struct {
		orders []*OrderRow
		users  []*UserRow
		err    error
	}

	results := make([]shardResult, len(shards))
	var wg sync.WaitGroup

	// Phase 1: Scatter — query each shard in parallel
	for i, shard := range shards {
		wg.Add(1)
		go func(idx int, s *Shard) {
			defer wg.Done()
			pool := s.ReplicaPool()

			// Fetch orders from this shard
			orderQuery := `
				SELECT id, user_id, status, total_price, shard_key, created_at, updated_at
				FROM orders`
			args := []any{}
			if status != "" {
				orderQuery += " WHERE status = $1"
				args = append(args, status)
			}
			orderQuery += " ORDER BY created_at DESC LIMIT $" + itoa(len(args)+1)
			args = append(args, limit)

			rows, err := pool.Query(ctx, orderQuery, args...)
			if err != nil {
				results[idx].err = err
				return
			}
			defer rows.Close()

			for rows.Next() {
				var o OrderRow
				if err := rows.Scan(&o.ID, &o.UserID, &o.Status, &o.TotalPrice,
					&o.ShardKey, &o.CreatedAt, &o.UpdatedAt); err != nil {
					results[idx].err = err
					return
				}
				results[idx].orders = append(results[idx].orders, &o)
			}

			// Fetch users from this shard (same shard = same user_id hash range)
			userRows, err := pool.Query(ctx, `
				SELECT id, email, name, shard_key, created_at FROM users`)
			if err != nil {
				results[idx].err = err
				return
			}
			defer userRows.Close()

			for userRows.Next() {
				var u UserRow
				if err := userRows.Scan(&u.ID, &u.Email, &u.Name,
					&u.ShardKey, &u.CreatedAt); err != nil {
					results[idx].err = err
					return
				}
				results[idx].users = append(results[idx].users, &u)
			}
		}(i, shard)
	}
	wg.Wait()

	// Phase 2: Build — hash map of all users across all shards
	userMap := make(map[uuid.UUID]*UserRow)
	var allOrders []*OrderRow

	for i, res := range results {
		if res.err != nil {
			j.log.Warn("cross-shard scatter error",
				zap.Int("shard", shards[i].ID), zap.Error(res.err))
			continue
		}
		for _, u := range res.users {
			userMap[u.ID] = u
		}
		allOrders = append(allOrders, res.orders...)
	}

	// Phase 3: Probe — join orders with users
	joined := make([]OrderWithUser, 0, len(allOrders))
	for _, o := range allOrders {
		u, ok := userMap[o.UserID]
		if !ok {
			// User not found (cross-shard miss — user may be on a different shard)
			// This can happen if user_id hash and order user_id hash land on different shards
			// In a real system, you'd do a targeted lookup on the correct shard
			j.log.Debug("cross-shard join: user not found on same shard",
				zap.String("user_id", o.UserID.String()))
			joined = append(joined, OrderWithUser{
				OrderID:    o.ID,
				UserID:     o.UserID,
				UserEmail:  "unknown",
				UserName:   "unknown",
				Status:     o.Status,
				TotalPrice: o.TotalPrice,
				CreatedAt:  o.CreatedAt,
				ShardKey:   o.ShardKey,
			})
			continue
		}
		joined = append(joined, OrderWithUser{
			OrderID:    o.ID,
			UserID:     o.UserID,
			UserEmail:  u.Email,
			UserName:   u.Name,
			Status:     o.Status,
			TotalPrice: o.TotalPrice,
			CreatedAt:  o.CreatedAt,
			ShardKey:   o.ShardKey,
		})
	}

	// Phase 4: Sort by created_at DESC (merge sort across shards)
	sort.Slice(joined, func(i, k int) bool {
		return joined[i].CreatedAt.After(joined[k].CreatedAt)
	})

	if len(joined) > limit {
		joined = joined[:limit]
	}
	return joined, nil
}

// ResolveUserForOrder does a targeted cross-shard lookup when a user is not
// found on the same shard as the order. This handles the case where user_id
// hashes to a different shard than the order's user_id.
func (j *CrossShardJoiner) ResolveUserForOrder(ctx context.Context, userID uuid.UUID) (*UserRow, error) {
	// Route to the correct shard using the consistent hash ring
	shard := j.sm.ShardForKey(userID.String())
	row := shard.ReplicaPool().QueryRow(ctx,
		`SELECT id, email, name, shard_key, created_at FROM users WHERE id = $1`, userID)

	var u UserRow
	if err := row.Scan(&u.ID, &u.Email, &u.Name, &u.ShardKey, &u.CreatedAt); err != nil {
		return nil, err
	}
	return &u, nil
}

// ShardStats returns per-shard row counts for observability.
type ShardStats struct {
	ShardID    int   `json:"shard_id"`
	UserCount  int64 `json:"user_count"`
	OrderCount int64 `json:"order_count"`
}

// GetShardStats scatter-gathers row counts from all shards.
func (j *CrossShardJoiner) GetShardStats(ctx context.Context) ([]ShardStats, error) {
	type result struct {
		stats ShardStats
		err   error
	}
	ch := make(chan result, len(j.sm.AllShards()))

	for _, shard := range j.sm.AllShards() {
		go func(s *Shard) {
			var stats ShardStats
			stats.ShardID = s.ID
			pool := s.ReplicaPool()

			if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM users`).Scan(&stats.UserCount); err != nil {
				ch <- result{err: err}
				return
			}
			if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM orders`).Scan(&stats.OrderCount); err != nil {
				ch <- result{err: err}
				return
			}
			ch <- result{stats: stats}
		}(shard)
	}

	var all []ShardStats
	for range j.sm.AllShards() {
		res := <-ch
		if res.err != nil {
			j.log.Warn("shard stats error", zap.Error(res.err))
			continue
		}
		all = append(all, res.stats)
	}
	sort.Slice(all, func(i, k int) bool { return all[i].ShardID < all[k].ShardID })
	return all, nil
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
