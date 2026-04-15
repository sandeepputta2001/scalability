// admin_handler.go exposes observability and diagnostic endpoints for all
// distributed systems components: PostgreSQL shards, replication lag,
// cross-shard joins, Redis cluster topology, hot keys, and cache inconsistencies.
package handlers

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/db"
	outboxPkg "github.com/distributed-ecommerce/internal/outbox"
)

type AdminHandler struct {
	joiner  *db.CrossShardJoiner
	monitor *db.ReplicationMonitor
	sm      *db.ShardManager
	cache   cache.CacheClient    // interface — works for both standalone and cluster
	cluster *cache.ClusterClient // nil in standalone mode
	log     *zap.Logger
}

func NewAdminHandler(
	joiner *db.CrossShardJoiner,
	monitor *db.ReplicationMonitor,
	sm *db.ShardManager,
	cacheClient cache.CacheClient,
	log *zap.Logger,
) *AdminHandler {
	h := &AdminHandler{
		joiner:  joiner,
		monitor: monitor,
		sm:      sm,
		cache:   cacheClient,
		log:     log,
	}
	// If cluster mode, keep a typed reference for cluster-specific endpoints
	if cc, ok := cacheClient.(*cache.ClusterClient); ok {
		h.cluster = cc
	}
	return h
}

// GetShardStats godoc
// GET /api/v1/admin/shards
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
		for i, lag := range ms.LagReport() {
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
		"note":              "replicas with lag > threshold are skipped for reads (fall back to primary)",
	})
}

// CrossShardOrdersWithUsers godoc
// GET /api/v1/admin/orders-with-users?status=pending&limit=50
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
		"note":    "scatter-gather across all PG shards + application-level hash join",
	})
}

// GetCacheInconsistencies godoc
// GET /api/v1/admin/cache-inconsistencies
func (h *AdminHandler) GetCacheInconsistencies(c *gin.Context) {
	events, err := h.cache.GetInconsistencyLog(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": events, "count": len(events)})
}

// GetCacheTagVersion godoc
// GET /api/v1/admin/cache-tag/:tag
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

// GetRedisClusterStats godoc
// GET /api/v1/admin/redis-cluster
// Returns per-node memory, keyspace, and replication info.
// Only available in cluster mode.
func (h *AdminHandler) GetRedisClusterStats(c *gin.Context) {
	if h.cluster == nil {
		c.JSON(http.StatusOK, gin.H{
			"mode": "standalone",
			"note": "set redis.cluster.enabled=true to use Redis Cluster",
		})
		return
	}
	stats, err := h.cluster.ClusterStats(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"mode":  "cluster",
		"nodes": stats,
		"note":  "3 masters × 1 replica = 6 nodes; 16384 slots distributed across masters",
	})
}

// GetHotKeys godoc
// GET /api/v1/admin/redis-hot-keys
// Returns the top-10 most accessed Redis keys in the current detection window.
// Hot keys are automatically promoted to the local in-process shard cache.
func (h *AdminHandler) GetHotKeys(c *gin.Context) {
	if h.cluster == nil {
		c.JSON(http.StatusOK, gin.H{
			"mode": "standalone",
			"note": "hot-key detection only available in cluster mode",
		})
		return
	}
	hotKeys := h.cluster.HotKeys()
	c.JSON(http.StatusOK, gin.H{
		"hot_keys": hotKeys,
		"note":     "keys above threshold are served from local in-process cache (zero Redis RTT)",
		"tradeoff": "local cache TTL=5s; stale reads possible within that window",
	})
}

// GetSlotInfo godoc
// GET /api/v1/admin/redis-slot?key=product:123
// Returns which cluster slot and node owns a given key.
func (h *AdminHandler) GetSlotInfo(c *gin.Context) {
	key := c.Query("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key query param required"})
		return
	}
	if h.cluster == nil {
		c.JSON(http.StatusOK, gin.H{"mode": "standalone", "note": "no slot routing in standalone mode"})
		return
	}
	info, err := h.cluster.SlotInfo(c.Request.Context(), key)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"slot_info": info})
}

// GetOutboxStats godoc
// GET /api/v1/admin/outbox-stats
// Returns per-status counts from the outbox_events table across all shards.
func (h *AdminHandler) GetOutboxStats(c *gin.Context) {
	// Scatter-gather outbox stats from all shards
	type shardStats struct {
		ShardID int              `json:"shard_id"`
		Counts  map[string]int64 `json:"counts"`
	}

	var results []shardStats
	for _, ms := range h.monitor.AllShards() {
		outboxRepo := outboxPkg.NewRepository(ms.Primary, h.log)
		counts, err := outboxRepo.GetStats(c.Request.Context())
		if err != nil {
			h.log.Warn("outbox stats error", zap.Int("shard", ms.ID), zap.Error(err))
			continue
		}
		results = append(results, shardStats{ShardID: ms.ID, Counts: counts})
	}
	c.JSON(http.StatusOK, gin.H{
		"shards": results,
		"note":   "pending=awaiting relay publish, published=delivered to Kafka, failed=max retries exceeded",
	})
}
