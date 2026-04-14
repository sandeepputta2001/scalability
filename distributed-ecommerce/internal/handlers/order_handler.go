package handlers

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/kafka"
	"github.com/distributed-ecommerce/internal/models"
	"github.com/distributed-ecommerce/internal/repository"
)

type OrderHandler struct {
	orderRepo   *repository.OrderRepository
	productRepo *repository.ProductRepository
	cache       cache.CacheClient
	producer    *kafka.Producer
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
		log:         log,
	}
}

// CreateOrder godoc
// POST /api/v1/orders
// Flow: distributed lock → validate stock → write PG → publish Kafka event → invalidate cache
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

	// Build order items, fetch prices from MongoDB
	var items []models.OrderItem
	var kafkaItems []kafka.OrderItem
	var total float64
	for _, ir := range req.Items {
		product, err := h.productRepo.GetByID(c.Request.Context(), ir.ProductID)
		if err != nil {
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
		kafkaItems = append(kafkaItems, kafka.OrderItem{
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

	if err := h.orderRepo.Create(c.Request.Context(), order); err != nil {
		h.log.Error("create order", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create order"})
		return
	}

	// Decrement stock + invalidate cache
	for _, item := range items {
		if err := h.productRepo.UpdateStock(c.Request.Context(), item.ProductID, -item.Quantity); err != nil {
			h.log.Warn("stock update failed", zap.String("product", item.ProductID), zap.Error(err))
		}
		_ = h.cache.InvalidateProduct(c.Request.Context(), item.ProductID)
	}
	_ = h.cache.DeleteCart(c.Request.Context(), userIDStr)

	// Publish order.created event to Kafka
	// Partition key = user_id → all events for this user land on the same partition
	correlationID := c.GetHeader("X-Correlation-ID")
	if correlationID == "" {
		correlationID = uuid.New().String()
	}
	env, err := kafka.NewEnvelope(
		kafka.EventOrderCreated,
		order.ID.String(),
		"order",
		correlationID,
		kafka.OrderCreatedEvent{
			OrderID:    order.ID.String(),
			UserID:     userIDStr,
			TotalPrice: order.TotalPrice,
			Items:      kafkaItems,
			ShardKey:   order.ShardKey,
		},
	)
	if err == nil {
		// Partition key = user_id for per-user ordering guarantee
		env.AggregateID = userIDStr
		if pubErr := h.producer.Publish(c.Request.Context(), kafka.TopicOrderCreated, env); pubErr != nil {
			// Non-fatal: order is already persisted in DB
			h.log.Warn("kafka publish failed for order.created", zap.Error(pubErr))
		}
	}

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
