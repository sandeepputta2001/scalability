package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/kafka"
	"github.com/distributed-ecommerce/internal/models"
	outboxpkg "github.com/distributed-ecommerce/internal/outbox"
	"github.com/distributed-ecommerce/internal/repository"
	"github.com/distributed-ecommerce/internal/retry"
)

type OrderHandler struct {
	orderRepo   *repository.OrderRepository
	productRepo *repository.ProductRepository
	cache       cache.CacheClient
	producer    *kafka.Producer
	kafkaCB     *retry.CircuitBreaker // circuit breaker for direct Kafka publishes
	log         *zap.Logger
}

func NewOrderHandler(
	orderRepo *repository.OrderRepository,
	productRepo *repository.ProductRepository,
	cache cache.CacheClient,
	producer *kafka.Producer,
	log *zap.Logger,
) *OrderHandler {
	return &OrderHandler{
		orderRepo:   orderRepo,
		productRepo: productRepo,
		cache:       cache,
		producer:    producer,
		// Circuit breaker: open after 5 consecutive Kafka failures,
		// probe again after 30s. While open, skip direct publish
		// (outbox relay will handle delivery).
		kafkaCB: retry.NewCircuitBreaker("kafka-publish", 5, 30e9, log),
		log:     log,
	}
}

// CreateOrder godoc
// POST /api/v1/orders
//
// Flow (Transactional Outbox):
//  1. Distributed lock (Redis) — prevent duplicate submissions
//  2. Fetch product prices from MongoDB (with retry)
//  3. INSERT order + INSERT outbox_event in ONE transaction (atomic)
//  4. Decrement stock in MongoDB (with retry)
//  5. Invalidate Redis cache
//  6. Try direct Kafka publish (best-effort, circuit-breaker protected)
//     → If Kafka is down: outbox relay will publish within poll interval
//  7. Return 201 to client
//
// Guarantee: order is NEVER created without a corresponding outbox event.
// Even if Kafka is completely down, the order is created and the event
// will be delivered when Kafka recovers.
func (h *OrderHandler) CreateOrder(c *gin.Context) {
	userIDStr := c.GetString("user_id")
	userID, err := uuid.Parse(userIDStr)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid user"})
		return
	}

	var req models.CreateOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Distributed lock: prevent duplicate order from same user within 5s
	lockKey := fmt.Sprintf("order-lock:%s", userIDStr)
	lockVal := uuid.New().String()
	acquired, err := h.cache.AcquireLock(c.Request.Context(), lockKey, lockVal, 5e9)
	if err != nil || !acquired {
		c.JSON(http.StatusConflict, gin.H{"error": "order already in progress, please wait"})
		return
	}
	defer h.cache.ReleaseLock(c.Request.Context(), lockKey, lockVal) //nolint:errcheck

	// Fetch product prices with retry (MongoDB may have transient failures)
	var items []models.OrderItem
	var total float64
	for _, ir := range req.Items {
		var product *models.Product
		fetchErr := retry.DoWithLog(
			c.Request.Context(), retry.DBConfig, h.log,
			fmt.Sprintf("fetch-product:%s", ir.ProductID),
			func(ctx context.Context) error {
				var e error
				product, e = h.productRepo.GetByID(ctx, ir.ProductID)
				return e
			},
		)
		if fetchErr != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("product %s not found", ir.ProductID)})
			return
		}
		if product.Stock < ir.Quantity {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("insufficient stock for %s", product.Name)})
			return
		}
		items = append(items, models.OrderItem{
			ProductID: ir.ProductID,
			Quantity:  ir.Quantity,
			UnitPrice: product.Price,
		})
		total += product.Price * float64(ir.Quantity)
	}

	order := &models.Order{
		UserID:     userID,
		Status:     models.OrderStatusPending,
		TotalPrice: total,
		Items:      items,
	}

	correlationID := c.GetHeader("X-Correlation-ID")
	if correlationID == "" {
		correlationID = uuid.New().String()
	}

	// Build the outbox event payload here (handler has kafka import, repo does not)
	kafkaItems := make([]kafka.OrderItem, len(items))
	for i, item := range items {
		kafkaItems[i] = kafka.OrderItem{
			ProductID: item.ProductID,
			Quantity:  item.Quantity,
			UnitPrice: item.UnitPrice,
		}
	}
	env, err := kafka.NewEnvelope(
		kafka.EventOrderCreated,
		userIDStr, // partition key = user_id
		"order",
		correlationID,
		kafka.OrderCreatedEvent{
			OrderID:    order.ID.String(), // will be overwritten after DB insert
			UserID:     userIDStr,
			TotalPrice: order.TotalPrice,
			Items:      kafkaItems,
		},
	)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build event"})
		return
	}
	envPayload, err := json.Marshal(env)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to marshal event"})
		return
	}
	outboxEvt := &outboxpkg.Event{
		AggregateType: "order",
		AggregateID:   "", // filled in by repo after order ID is assigned
		EventType:     string(kafka.EventOrderCreated),
		Payload:       envPayload,
		Topic:         kafka.TopicOrderCreated,
		PartitionKey:  userIDStr,
	}

	// Atomic write: order + outbox event in one transaction
	if err := h.orderRepo.Create(c.Request.Context(), order, outboxEvt); err != nil {
		h.log.Error("create order", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create order"})
		return
	}

	// Decrement stock with retry
	for _, item := range items {
		stockErr := retry.DoWithLog(
			c.Request.Context(), retry.DBConfig, h.log,
			fmt.Sprintf("update-stock:%s", item.ProductID),
			func(ctx context.Context) error {
				return h.productRepo.UpdateStock(ctx, item.ProductID, -item.Quantity)
			},
		)
		if stockErr != nil {
			h.log.Warn("stock update failed after retries",
				zap.String("product", item.ProductID), zap.Error(stockErr))
		}
		_ = h.cache.InvalidateProduct(c.Request.Context(), item.ProductID)
	}
	_ = h.cache.DeleteCart(c.Request.Context(), userIDStr)

	// Best-effort direct Kafka publish (circuit-breaker protected).
	// If this fails, the outbox relay will publish within the poll interval.
	// The circuit breaker prevents hammering a down Kafka cluster.
	go func() {
		env, err := kafka.NewEnvelope(
			kafka.EventOrderCreated,
			userIDStr,
			"order",
			correlationID,
			kafka.OrderCreatedEvent{
				OrderID:    order.ID.String(),
				UserID:     userIDStr,
				TotalPrice: order.TotalPrice,
				ShardKey:   order.ShardKey,
			},
		)
		if err != nil {
			return
		}
		cbErr := h.kafkaCB.Do(c.Request.Context(), func(ctx context.Context) error {
			return h.producer.Publish(ctx, kafka.TopicOrderCreated, env)
		})
		if cbErr != nil {
			h.log.Debug("direct kafka publish skipped/failed, outbox relay will handle",
				zap.String("order_id", order.ID.String()),
				zap.String("cb_state", h.kafkaCB.State().String()),
				zap.Error(cbErr))
		}
	}()

	c.JSON(http.StatusCreated, order)
}

// GetOrder godoc
// GET /api/v1/orders/:id
func (h *OrderHandler) GetOrder(c *gin.Context) {
	userIDStr := c.GetString("user_id")
	userID, _ := uuid.Parse(userIDStr)
	orderID, err := uuid.Parse(c.Param("id"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid order id"})
		return
	}
	order, err := h.orderRepo.GetByID(c.Request.Context(), orderID, userID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "order not found"})
		return
	}
	c.JSON(http.StatusOK, order)
}

// ListOrders godoc
// GET /api/v1/orders
func (h *OrderHandler) ListOrders(c *gin.Context) {
	userID, _ := uuid.Parse(c.GetString("user_id"))
	orders, err := h.orderRepo.ListByUser(c.Request.Context(), userID)
	if err != nil {
		h.log.Error("list orders", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list orders"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"orders": orders})
}
