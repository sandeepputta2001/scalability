// admin_handler.go exposes observability and diagnostic endpoints that
// demonstrate the distributed systems tradeoffs in action.
package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/db"
)

type AdminHandler struct {
	joiner  *db.CrossShardJoiner
	monitor *db.ReplicationMonitor
	sm      *db.ShardManager
	cache   *cache.Client
	log     *zap.Logger
}

func NewAdminHandler(
	joiner *db.CrossShardJoiner,
	monitor *db.ReplicationMonitor,
	sm *db.ShardManager,
	cache *cache.Client,
	log *zap.Logger,
) *AdminHandler {
	return &AdminHandler{joiner: joiner, monitor: monitor, sm: sm, cache: cache, log: log}
}

// GetShardStats godoc
// GET /api/v1/admin/shards
// Returns per-shard row counts and ring distribution.
func (h *AdminHandler) GetShardStats(c *gin.Context) {
	stats, err := h.joiner.GetShardStats(c.Request.Context())
	if err != nil {
		h.log.Error("shard stats", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"shards":            stats,
		"ring_distribution": h.sm.RingDistribution(),
		"total_shards":      h.sm.ShardCount(),
	})
}

// GetReplicationLag godoc
// GET /api/v1/admin/replication-lag
// Returns current replication lag per shard/replica and degraded status.
//
// This endpoint makes the replication lag tradeoff visible:
// if lag_seconds > 5, that replica is skipped for reads (reads go to primary).
func (h *AdminHandler) GetReplicationLag(c *gin.Context) {
	type replicaInfo struct {
		Index      int     `json:"index"`
		LagSeconds float64 `json:"lag_seconds"`
		Degraded   bool    `json:"degraded"`
	}
	type shardInfo struct {
		ShardID  int           `json:"shard_id"`
		Replicas []replicaInfo `json:"replicas"`
	}

	var shards []shardInfo
	for _, ms := range h.monitor.AllShards() {
		si := shardInfo{ShardID: ms.ID}
		lags := ms.LagReport()
		for i, lag := range lags {
			si.Replicas = append(si.Replicas, replicaInfo{
				Index:      i,
				LagSeconds: lag,
				Degraded:   lag > db.MaxLagThreshold,
			})
		}
		shards = append(shards, si)
	}
	c.JSON(http.StatusOK, gin.H{
		"shards":            shards,
		"max_lag_threshold": db.MaxLagThreshold,
		"note":              "replicas with lag > max_lag_threshold are skipped for reads",
	})
}

// CrossShardOrdersWithUsers godoc
// GET /api/v1/admin/orders-with-users?status=pending&limit=50
//
// Demonstrates the scatter-gather + application-level hash join pattern.
// This is the only way to JOIN data across shards — there is no cross-shard SQL.
func (h *AdminHandler) CrossShardOrdersWithUsers(c *gin.Context) {
	status := c.Query("status")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit > 200 {
		limit = 200
	}

	joined, err := h.joiner.OrdersWithUsers(c.Request.Context(), status, limit)
	if err != nil {
		h.log.Error("cross-shard join", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"results": joined,
		"count":   len(joined),
		"note":    "scatter-gather across all shards + application-level hash join",
	})
}

// GetCacheInconsistencies godoc
// GET /api/v1/admin/cache-inconsistencies
// Returns the last 100 detected cache/DB inconsistency events.
func (h *AdminHandler) GetCacheInconsistencies(c *gin.Context) {
	events, err := h.cache.GetInconsistencyLog(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"events": events,
		"count":  len(events),
		"note":   "cache/db inconsistencies detected during read-verify operations",
	})
}

// GetCacheTagVersion godoc
// GET /api/v1/admin/cache-tag/:tag
// Returns the current version for a cache tag.
func (h *AdminHandler) GetCacheTagVersion(c *gin.Context) {
	tag := c.Param("tag")
	version, err := h.cache.TagVersion(c.Request.Context(), tag)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"tag": tag, "version": version})
}

// BumpCacheTag godoc
// POST /api/v1/admin/cache-tag/:tag/bump
// Bumps the version for a cache tag, effectively invalidating all tagged keys.
func (h *AdminHandler) BumpCacheTag(c *gin.Context) {
	tag := c.Param("tag")
	newVersion, err := h.cache.BumpTagVersion(c.Request.Context(), tag)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"tag":         tag,
		"new_version": newVersion,
		"note":        "all keys with previous version are now effectively invalidated",
	})
}
