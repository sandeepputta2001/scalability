// replication_monitor.go tracks per-replica replication lag and exposes
// lag-aware read routing.
//
// Replication Lag Tradeoff:
//
//	Reading from a lagged replica can return stale data. This is the
//	fundamental consistency vs. availability tradeoff in async replication.
//
//	Strategy implemented here:
//	  1. Background goroutine polls each replica every LagCheckInterval.
//	  2. If a replica's lag exceeds MaxLagThreshold, it is marked "degraded".
//	  3. ReplicaPool() skips degraded replicas and falls back to primary.
//	  4. This gives us "read-your-writes" safety at the cost of higher
//	     primary load during lag spikes.
//
//	Alternative (not implemented): route reads to primary only when the
//	client has a "strong consistency" header — letting most reads tolerate
//	eventual consistency while critical reads are always fresh.
package db

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

const (
	LagCheckInterval = 10 * time.Second
	MaxLagThreshold  = 5.0 // seconds — replicas lagging more than this are skipped
)

// ReplicaHealth tracks the health of a single replica pool.
type ReplicaHealth struct {
	Pool       *pgxpool.Pool
	LagSeconds atomic.Value // stores float64
	Degraded   atomic.Bool
}

func newReplicaHealth(pool *pgxpool.Pool) *ReplicaHealth {
	rh := &ReplicaHealth{Pool: pool}
	rh.LagSeconds.Store(float64(0))
	return rh
}

// ReplicationMonitor manages health state for all replicas across all shards.
type ReplicationMonitor struct {
	shards []*MonitoredShard
	log    *zap.Logger
	stopCh chan struct{}
	once   sync.Once
}

// MonitoredShard wraps a Shard with per-replica health tracking.
type MonitoredShard struct {
	ID       int
	Primary  *pgxpool.Pool
	Replicas []*ReplicaHealth
	rrIndex  atomic.Uint64
}

// ReplicaPool returns the next healthy replica using round-robin.
// Falls back to primary if all replicas are degraded.
func (s *MonitoredShard) ReplicaPool() *pgxpool.Pool {
	healthy := make([]*ReplicaHealth, 0, len(s.Replicas))
	for _, r := range s.Replicas {
		if !r.Degraded.Load() {
			healthy = append(healthy, r)
		}
	}
	if len(healthy) == 0 {
		return s.Primary // all replicas degraded — fall back to primary
	}
	idx := s.rrIndex.Add(1) % uint64(len(healthy))
	return healthy[idx].Pool
}

// LagReport returns lag seconds for each replica.
func (s *MonitoredShard) LagReport() []float64 {
	lags := make([]float64, len(s.Replicas))
	for i, r := range s.Replicas {
		lags[i] = r.LagSeconds.Load().(float64)
	}
	return lags
}

// NewReplicationMonitor wraps the ShardManager's shards with health tracking.
func NewReplicationMonitor(sm *ShardManager, log *zap.Logger) *ReplicationMonitor {
	rm := &ReplicationMonitor{log: log, stopCh: make(chan struct{})}
	for _, s := range sm.shards {
		ms := &MonitoredShard{ID: s.ID, Primary: s.Primary}
		for _, r := range s.Replicas {
			ms.Replicas = append(ms.Replicas, newReplicaHealth(r))
		}
		rm.shards = append(rm.shards, ms)
	}
	return rm
}

// Start launches the background lag-polling loop.
func (rm *ReplicationMonitor) Start() {
	rm.once.Do(func() {
		go rm.pollLoop()
	})
}

// Stop shuts down the polling loop.
func (rm *ReplicationMonitor) Stop() {
	close(rm.stopCh)
}

// ShardByID returns the monitored shard for a given shard ID.
func (rm *ReplicationMonitor) ShardByID(id int) *MonitoredShard {
	for _, s := range rm.shards {
		if s.ID == id {
			return s
		}
	}
	return rm.shards[0]
}

// AllShards returns all monitored shards.
func (rm *ReplicationMonitor) AllShards() []*MonitoredShard {
	return rm.shards
}

func (rm *ReplicationMonitor) pollLoop() {
	ticker := time.NewTicker(LagCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rm.checkAllReplicas()
		case <-rm.stopCh:
			return
		}
	}
}

func (rm *ReplicationMonitor) checkAllReplicas() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, shard := range rm.shards {
		for i, replica := range shard.Replicas {
			var lagSeconds float64
			err := replica.Pool.QueryRow(ctx, `
				SELECT COALESCE(
					EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp())),
					0
				)`).Scan(&lagSeconds)

			if err != nil {
				// Can't reach replica — mark degraded
				replica.Degraded.Store(true)
				replica.LagSeconds.Store(float64(9999))
				rm.log.Warn("replica unreachable",
					zap.Int("shard", shard.ID), zap.Int("replica", i), zap.Error(err))
				continue
			}

			replica.LagSeconds.Store(lagSeconds)
			wasDegraded := replica.Degraded.Load()
			isDegraded := lagSeconds > MaxLagThreshold
			replica.Degraded.Store(isDegraded)

			if isDegraded && !wasDegraded {
				rm.log.Warn("replica marked degraded due to lag",
					zap.Int("shard", shard.ID), zap.Int("replica", i),
					zap.Float64("lag_seconds", lagSeconds))
			} else if !isDegraded && wasDegraded {
				rm.log.Info("replica recovered",
					zap.Int("shard", shard.ID), zap.Int("replica", i),
					zap.Float64("lag_seconds", lagSeconds))
			}
		}
	}
}
