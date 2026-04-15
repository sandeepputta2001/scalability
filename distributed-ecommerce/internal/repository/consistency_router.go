// consistency_router.go provides a consistency-aware pool selector for
// PostgreSQL reads. It reads the consistency level from the context and
// routes to either the primary or a replica accordingly.
//
// This is the integration point between the consistency package and the
// actual database connection pools.
package repository

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/distributed-ecommerce/internal/consistency"
	"github.com/distributed-ecommerce/internal/db"
)

// ReadPool returns the appropriate connection pool for a read operation
// based on the consistency level in the context.
//
// Strong / Session (recent write) → primary pool
// BoundedStaleness                → replica if lag OK, else primary
// Eventual                        → replica (round-robin)
func ReadPool(ctx context.Context, shard *db.Shard, monitor *db.ReplicationMonitor) *pgxpool.Pool {
	ms := monitor.ShardByID(shard.ID)

	getLag := func(shardID, replicaIdx int) float64 {
		lags := ms.LagReport()
		if replicaIdx < len(lags) {
			return lags[replicaIdx]
		}
		return 0
	}

	target := consistency.RouteRead(ctx, shard.ID, getLag)
	if target == consistency.ReadFromPrimary {
		return shard.Primary
	}
	return ms.ReplicaPool()
}

// WritePool always returns the primary pool.
// Writes always go to the primary regardless of consistency level.
// The consistency level affects write concern (acks), not routing.
func WritePool(shard *db.Shard) *pgxpool.Pool {
	return shard.Primary
}
