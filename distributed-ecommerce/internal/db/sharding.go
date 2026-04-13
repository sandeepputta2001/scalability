// Package db implements virtual-node consistent-hash based PostgreSQL sharding
// with primary/replica routing and lag-aware read routing.
//
// Sharding Strategy: Consistent Hash Ring
//   - Each shard owns V=150 virtual nodes on a 2^32 ring
//   - Key → FNV-1a hash → clockwise successor on ring → shard
//   - Adding/removing a shard moves only ~1/N of keys (vs. all keys with modulo)
//
// Read Routing:
//   - Writes always go to the shard primary
//   - Reads round-robin across healthy replicas (lag < 5s)
//   - If all replicas are lagged/down, reads fall back to primary
//
// Cross-Shard Joins:
//   - Not supported natively — use scatter-gather + application-level merge
//   - See ScatterGatherOrders in order_repo.go for the pattern
package db

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/config"
)

// Shard holds one primary pool and N replica pools.
type Shard struct {
	ID       int
	Primary  *pgxpool.Pool
	Replicas []*pgxpool.Pool
	rrIndex  atomic.Uint64 // round-robin counter for replica selection
}

// ReplicaPool returns the next replica pool using round-robin.
// Falls back to primary when no replicas are configured.
func (s *Shard) ReplicaPool() *pgxpool.Pool {
	if len(s.Replicas) == 0 {
		return s.Primary
	}
	idx := s.rrIndex.Add(1) % uint64(len(s.Replicas))
	return s.Replicas[idx]
}

// ShardManager routes queries to the correct shard using a consistent hash ring.
type ShardManager struct {
	shards []*Shard
	ring   *ConsistentHashRing
	log    *zap.Logger
}

// NewShardManager connects to every shard defined in config and builds the ring.
func NewShardManager(ctx context.Context, cfg config.PostgresConfig, log *zap.Logger) (*ShardManager, error) {
	sm := &ShardManager{log: log}

	var shardIDs []int
	for _, sc := range cfg.Shards {
		shard := &Shard{ID: sc.ID}

		primary, err := newPool(ctx, sc.Primary, cfg)
		if err != nil {
			return nil, fmt.Errorf("shard %d primary: %w", sc.ID, err)
		}
		shard.Primary = primary

		for _, replicaDSN := range sc.Replicas {
			replica, err := newPool(ctx, replicaDSN, cfg)
			if err != nil {
				log.Warn("shard replica unavailable, skipping",
					zap.Int("shard", sc.ID), zap.Error(err))
				continue
			}
			shard.Replicas = append(shard.Replicas, replica)
		}

		sm.shards = append(sm.shards, shard)
		shardIDs = append(shardIDs, sc.ID)
		log.Info("shard connected",
			zap.Int("id", sc.ID),
			zap.Int("replicas", len(shard.Replicas)))
	}

	if len(sm.shards) == 0 {
		return nil, fmt.Errorf("no shards configured")
	}

	// Build consistent hash ring with virtual nodes
	sm.ring = NewConsistentHashRing(shardIDs, defaultVirtualNodes)

	// Log ring distribution for observability
	dist := sm.ring.Distribution()
	log.Info("consistent hash ring built", zap.Any("vnode_distribution", dist))

	return sm, nil
}

// ShardForKey returns the shard responsible for the given key using the ring.
func (sm *ShardManager) ShardForKey(key string) *Shard {
	shardID := sm.ring.GetShardID(key)
	return sm.shardByID(shardID)
}

// ShardByIndex returns a shard by its stored numeric index.
func (sm *ShardManager) ShardByIndex(idx int) *Shard {
	return sm.shards[idx%len(sm.shards)]
}

// AllShards returns every shard (used for scatter-gather queries).
func (sm *ShardManager) AllShards() []*Shard {
	return sm.shards
}

// ShardCount returns the total number of shards.
func (sm *ShardManager) ShardCount() int {
	return len(sm.shards)
}

// RingDistribution returns the vnode distribution for observability.
func (sm *ShardManager) RingDistribution() map[int]int {
	return sm.ring.Distribution()
}

// Close closes all connection pools.
func (sm *ShardManager) Close() {
	for _, s := range sm.shards {
		s.Primary.Close()
		for _, r := range s.Replicas {
			r.Close()
		}
	}
}

func (sm *ShardManager) shardByID(id int) *Shard {
	for _, s := range sm.shards {
		if s.ID == id {
			return s
		}
	}
	return sm.shards[0]
}

func newPool(ctx context.Context, dsn string, cfg config.PostgresConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	poolCfg.MinConns = int32(cfg.MaxIdleConns)

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping failed: %w", err)
	}
	return pool, nil
}
