// lb_partition_handler.go exposes load balancer stats, partition distribution
// reports, and consistency diagnostics via admin endpoints.
package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/loadbalancer"
	"github.com/distributed-ecommerce/internal/partition"
)

type LBPartitionHandler struct {
	replicaLB     *loadbalancer.LoadBalancer // DB replica load balancer
	mongoLB       *loadbalancer.LoadBalancer // MongoDB node load balancer
	orderRouter   *partition.Router          // order partitioning router
	userRouter    *partition.Router          // user partitioning router
	productRouter *partition.Router          // product partitioning router
	log           *zap.Logger
}

func NewLBPartitionHandler(
	replicaLB, mongoLB *loadbalancer.LoadBalancer,
	orderRouter, userRouter, productRouter *partition.Router,
	log *zap.Logger,
) *LBPartitionHandler {
	return &LBPartitionHandler{
		replicaLB:     replicaLB,
		mongoLB:       mongoLB,
		orderRouter:   orderRouter,
		userRouter:    userRouter,
		productRouter: productRouter,
		log:           log,
	}
}

// GetLBStats godoc
// GET /api/v1/admin/lb-stats
// Returns per-node health, active connections, and algorithm for each LB.
func (h *LBPartitionHandler) GetLBStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"db_replicas": h.replicaLB.Stats(),
		"mongodb":     h.mongoLB.Stats(),
		"algorithms": gin.H{
			"db_replicas": "weighted-round-robin",
			"mongodb":     "least-connections",
		},
		"note": "active_conns shows in-flight requests per node; unhealthy nodes are skipped",
	})
}

// GetPartitionReport godoc
// GET /api/v1/admin/partition-report
// Returns data distribution across partitions for orders, users, and products.
// Skew > 0.2 triggers a rebalance recommendation.
func (h *LBPartitionHandler) GetPartitionReport(c *gin.Context) {
	orderReport := h.orderRouter.Report()
	userReport := h.userRouter.Report()
	productReport := h.productRouter.Report()

	c.JSON(http.StatusOK, gin.H{
		"orders": gin.H{
			"strategy": h.orderRouter.Strategy(),
			"report":   orderReport,
		},
		"users": gin.H{
			"strategy": h.userRouter.Strategy(),
			"report":   userReport,
		},
		"products": gin.H{
			"strategy": h.productRouter.Strategy(),
			"report":   productReport,
		},
		"note": "skew_coefficient > 0.2 means uneven distribution; rebalance_recommended=true suggests action",
	})
}

// RouteKey godoc
// GET /api/v1/admin/route-key?key=user@example.com&entity=user
// Shows which partition a key routes to under each strategy.
func (h *LBPartitionHandler) RouteKey(c *gin.Context) {
	key := c.Query("key")
	entity := c.DefaultQuery("entity", "order")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "key param required"})
		return
	}

	var router *partition.Router
	switch entity {
	case "user":
		router = h.userRouter
	case "product":
		router = h.productRouter
	default:
		router = h.orderRouter
	}

	c.JSON(http.StatusOK, gin.H{
		"routing": router.KeyInfo(key),
		"note":    "shows which partition this key maps to under the current strategy",
	})
}
