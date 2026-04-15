// patterns_handler.go exposes observability endpoints for all design patterns:
// bulkhead stats, saga execution history, event bus subscriptions,
// circuit breaker states, and CQRS command/query routing info.
package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/patterns"
	"github.com/distributed-ecommerce/internal/retry"
)

// PatternsHandler exposes design pattern observability.
type PatternsHandler struct {
	bulkheads       *patterns.BulkheadRegistry
	cmdBus          *patterns.CommandBus
	queryBus        *patterns.QueryBus
	orchestrator    *patterns.Orchestrator
	circuitBreakers []*retry.CircuitBreaker
	log             *zap.Logger
}

func NewPatternsHandler(
	bulkheads *patterns.BulkheadRegistry,
	cmdBus *patterns.CommandBus,
	queryBus *patterns.QueryBus,
	orchestrator *patterns.Orchestrator,
	cbs []*retry.CircuitBreaker,
	log *zap.Logger,
) *PatternsHandler {
	return &PatternsHandler{
		bulkheads:       bulkheads,
		cmdBus:          cmdBus,
		queryBus:        queryBus,
		orchestrator:    orchestrator,
		circuitBreakers: cbs,
		log:             log,
	}
}

// GetBulkheadStats godoc
// GET /api/v1/admin/bulkheads
// Returns per-bulkhead capacity, in-use, and rejection counts.
func (h *PatternsHandler) GetBulkheadStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"bulkheads": h.bulkheads.AllStats(),
		"note":      "in_use shows current concurrent requests; total_rejected shows requests turned away",
	})
}

// GetCircuitBreakerStats godoc
// GET /api/v1/admin/circuit-breakers
func (h *PatternsHandler) GetCircuitBreakerStats(c *gin.Context) {
	stats := make([]retry.CircuitBreakerStats, 0, len(h.circuitBreakers))
	for _, cb := range h.circuitBreakers {
		stats = append(stats, cb.Stats())
	}
	c.JSON(http.StatusOK, gin.H{
		"circuit_breakers": stats,
		"states":           "closed=normal, open=fast-fail, half-open=probing",
	})
}

// RunOrderSaga godoc
// POST /api/v1/admin/run-saga
// Runs a demo order placement saga and returns the execution trace.
// This demonstrates the saga pattern with compensating transactions.
func (h *PatternsHandler) RunOrderSaga(c *gin.Context) {
	var req struct {
		UserID string `json:"user_id" binding:"required"`
		Fail   string `json:"fail_at"` // step name to simulate failure
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userID, err := uuid.Parse(req.UserID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid user_id"})
		return
	}

	initialState := map[string]any{
		"user_id":  userID.String(),
		"order_id": uuid.New().String(),
		"fail_at":  req.Fail,
	}

	// Build saga steps with simulated actions
	steps := patterns.BuildOrderPlacementSaga(
		makeStep("ReserveStock", req.Fail),
		makeCompensation("ReleaseStock"),
		makeStep("CreateOrder", req.Fail),
		makeCompensation("CancelOrder"),
		makeStep("ChargePayment", req.Fail),
		makeCompensation("RefundPayment"),
		makeStep("ConfirmOrder", req.Fail),
	)

	exec := h.orchestrator.Execute(c.Request.Context(), "OrderPlacement", steps, initialState)

	c.JSON(http.StatusOK, gin.H{
		"saga_execution": exec,
		"note":           "set fail_at to a step name to simulate failure and observe compensation",
	})
}

// DemoSpecification godoc
// GET /api/v1/admin/spec-demo?category=electronics&max_price=500&tag=laptop
// Demonstrates the Specification pattern by building a composite spec.
func (h *PatternsHandler) DemoSpecification(c *gin.Context) {
	category := c.DefaultQuery("category", "electronics")
	maxPrice := 500.0
	tag := c.DefaultQuery("tag", "")

	var spec patterns.Specification = patterns.InCategory(category)

	if maxPrice > 0 {
		spec = spec.(*patterns.CategorySpec).And(patterns.PriceBelow(maxPrice))
	}
	if tag != "" {
		spec = spec.(*patterns.AndSpec).And(patterns.HasTag(tag))
	}
	spec = spec.(*patterns.AndSpec).And(patterns.InStock())

	mongoFilter := spec.ToMongoFilter()
	sqlClause, sqlArgs := spec.ToSQLWhere()

	c.JSON(http.StatusOK, gin.H{
		"description":  spec.Description(),
		"mongo_filter": mongoFilter,
		"sql_where":    sqlClause,
		"sql_args":     sqlArgs,
		"note":         "specifications compose business rules into reusable, testable predicates",
	})
}

// DemoCQRS godoc
// POST /api/v1/admin/cqrs-demo
// Demonstrates the CQRS pattern by dispatching a command and a query.
func (h *PatternsHandler) DemoCQRS(c *gin.Context) {
	// Demo command
	cmd := patterns.CreateOrderCommand{
		UserID: uuid.New(),
		Items: []patterns.OrderItemSpec{
			{ProductID: "prod-1", Quantity: 2, UnitPrice: 99.99},
		},
		CorrelationID: uuid.New().String(),
	}

	if err := cmd.Validate(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"command_validation_error": err.Error()})
		return
	}

	// Demo query
	q := patterns.GetProductQuery{
		ProductID:   "prod-1",
		Consistency: "eventual",
	}

	c.JSON(http.StatusOK, gin.H{
		"command": gin.H{
			"name":    cmd.CommandName(),
			"valid":   true,
			"user_id": cmd.UserID,
			"items":   len(cmd.Items),
		},
		"query": gin.H{
			"name":        q.QueryName(),
			"product_id":  q.ProductID,
			"consistency": q.Consistency,
		},
		"note": "commands mutate state (write path), queries read state (read path) — never mixed",
	})
}

// DemoBuilder godoc
// POST /api/v1/admin/builder-demo
// Demonstrates the Builder pattern for constructing complex objects.
func (h *PatternsHandler) DemoBuilder(c *gin.Context) {
	// Valid order
	order, err := patterns.NewOrderBuilder(uuid.New()).
		AddItem("prod-1", 2, 99.99).
		AddItem("prod-2", 1, 49.99).
		WithStatus("pending").
		Build()

	// Invalid product (demonstrates validation)
	_, productErr := patterns.NewProductBuilder("", -10, "").Build()

	c.JSON(http.StatusOK, gin.H{
		"valid_order": gin.H{
			"user_id":     order.UserID,
			"item_count":  len(order.Items),
			"total_price": order.TotalPrice,
			"status":      order.Status,
		},
		"invalid_product_error": productErr.Error(),
		"build_error":           err,
		"note":                  "builder validates all fields at Build() time, preventing invalid state",
	})
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func makeStep(name, failAt string) func(ctx context.Context, state map[string]any) error {
	return func(ctx context.Context, state map[string]any) error {
		if failAt == name {
			return fmt.Errorf("simulated failure at step: %s", name)
		}
		state[name+"_done"] = true
		return nil
	}
}

func makeCompensation(name string) func(ctx context.Context, state map[string]any) error {
	return func(ctx context.Context, state map[string]any) error {
		state[name+"_compensated"] = true
		return nil
	}
}
