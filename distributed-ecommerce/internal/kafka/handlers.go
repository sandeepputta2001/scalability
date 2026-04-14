// handlers.go contains the business logic executed by each consumer group.
// Each handler receives a decoded Envelope and performs the appropriate action.
//
// Handler responsibilities:
//
//	OrderProcessor:    update order status in PostgreSQL, trigger next event
//	StockProcessor:    update product stock in MongoDB, invalidate Redis cache
//	CacheInvalidator:  invalidate Redis cache keys on any data-changing event
//	NotificationSvc:   (stub) send email/push notifications
//	AnalyticsSvc:      (stub) write events to analytics store
package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/models"
	"github.com/distributed-ecommerce/internal/repository"
	"github.com/google/uuid"
)

// ─── Order Processor ─────────────────────────────────────────────────────────

// OrderProcessorHandler handles order lifecycle events.
// It updates order status in the correct PostgreSQL shard.
type OrderProcessorHandler struct {
	orderRepo *repository.OrderRepository
	producer  *Producer
	log       *zap.Logger
}

func NewOrderProcessorHandler(
	orderRepo *repository.OrderRepository,
	producer *Producer,
	log *zap.Logger,
) *OrderProcessorHandler {
	return &OrderProcessorHandler{orderRepo: orderRepo, producer: producer, log: log}
}

func (h *OrderProcessorHandler) Handle(ctx context.Context, env *Envelope) error {
	switch env.EventType {
	case EventOrderCreated:
		return h.handleOrderCreated(ctx, env)
	case EventOrderConfirmed:
		return h.handleStatusChange(ctx, env, models.OrderStatusConfirmed)
	case EventOrderShipped:
		return h.handleStatusChange(ctx, env, models.OrderStatusShipped)
	case EventOrderCancelled:
		return h.handleStatusChange(ctx, env, models.OrderStatusCancelled)
	default:
		h.log.Warn("order processor: unknown event type", zap.String("type", string(env.EventType)))
		return nil
	}
}

func (h *OrderProcessorHandler) handleOrderCreated(ctx context.Context, env *Envelope) error {
	var evt OrderCreatedEvent
	if err := json.Unmarshal(env.Payload, &evt); err != nil {
		return fmt.Errorf("unmarshal order created: %w", err)
	}
	h.log.Info("order created event processed",
		zap.String("order_id", evt.OrderID),
		zap.String("user_id", evt.UserID),
		zap.Float64("total", evt.TotalPrice))

	// Auto-confirm after 2 minutes (simulate payment gateway callback)
	// In production this would be triggered by a payment webhook
	go func() {
		time.Sleep(2 * time.Minute)
		confirmEnv, err := NewEnvelope(
			EventOrderConfirmed, evt.OrderID, "order", env.CorrelationID,
			OrderStatusChangedEvent{
				OrderID:   evt.OrderID,
				UserID:    evt.UserID,
				OldStatus: string(models.OrderStatusPending),
				NewStatus: string(models.OrderStatusConfirmed),
				ShardKey:  evt.ShardKey,
			},
		)
		if err != nil {
			h.log.Error("build confirm event", zap.Error(err))
			return
		}
		if err := h.producer.Publish(context.Background(), TopicOrderConfirmed, confirmEnv); err != nil {
			h.log.Error("publish confirm event", zap.Error(err))
		}
	}()
	return nil
}

func (h *OrderProcessorHandler) handleStatusChange(ctx context.Context, env *Envelope, status models.OrderStatus) error {
	var evt OrderStatusChangedEvent
	if err := json.Unmarshal(env.Payload, &evt); err != nil {
		return fmt.Errorf("unmarshal status change: %w", err)
	}

	orderID, err := uuid.Parse(evt.OrderID)
	if err != nil {
		return fmt.Errorf("parse order_id: %w", err)
	}
	userID, err := uuid.Parse(evt.UserID)
	if err != nil {
		return fmt.Errorf("parse user_id: %w", err)
	}

	if err := h.orderRepo.UpdateStatus(ctx, orderID, userID, status); err != nil {
		return fmt.Errorf("update order status: %w", err)
	}
	h.log.Info("order status updated",
		zap.String("order_id", evt.OrderID),
		zap.String("status", string(status)))
	return nil
}

// ─── Stock Processor ──────────────────────────────────────────────────────────

// StockProcessorHandler handles stock update events from MongoDB.
type StockProcessorHandler struct {
	productRepo *repository.ProductRepository
	cache       cache.CacheClient
	log         *zap.Logger
}

func NewStockProcessorHandler(
	productRepo *repository.ProductRepository,
	cache cache.CacheClient,
	log *zap.Logger,
) *StockProcessorHandler {
	return &StockProcessorHandler{productRepo: productRepo, cache: cache, log: log}
}

func (h *StockProcessorHandler) Handle(ctx context.Context, env *Envelope) error {
	if env.EventType != EventStockUpdated {
		return nil
	}
	var evt StockUpdatedEvent
	if err := json.Unmarshal(env.Payload, &evt); err != nil {
		return fmt.Errorf("unmarshal stock updated: %w", err)
	}

	// Invalidate product cache — next read will fetch fresh data from MongoDB
	if err := h.cache.InvalidateProduct(ctx, evt.ProductID); err != nil {
		h.log.Warn("cache invalidation failed", zap.String("product_id", evt.ProductID), zap.Error(err))
	}

	h.log.Info("stock event processed",
		zap.String("product_id", evt.ProductID),
		zap.Int("delta", evt.Delta),
		zap.Int("new_stock", evt.NewStock))
	return nil
}

// ─── Cache Invalidator ────────────────────────────────────────────────────────

// CacheInvalidatorHandler listens to all data-changing events and invalidates
// the corresponding Redis cache keys. This is the "event-driven cache invalidation"
// pattern — more reliable than synchronous invalidation in the write path because
// it handles cases where the write path crashes after DB write but before cache delete.
//
// Consistency guarantee: cache will be invalidated eventually (within Kafka lag).
// Inconsistency window: time between DB write and Kafka consumer processing.
type CacheInvalidatorHandler struct {
	cache cache.CacheClient
	log   *zap.Logger
}

func NewCacheInvalidatorHandler(cache cache.CacheClient, log *zap.Logger) *CacheInvalidatorHandler {
	return &CacheInvalidatorHandler{cache: cache, log: log}
}

func (h *CacheInvalidatorHandler) Handle(ctx context.Context, env *Envelope) error {
	switch env.EventType {
	case EventStockUpdated:
		var evt StockUpdatedEvent
		if err := json.Unmarshal(env.Payload, &evt); err != nil {
			return err
		}
		return h.cache.InvalidateProduct(ctx, evt.ProductID)

	case EventOrderCreated, EventOrderConfirmed, EventOrderShipped, EventOrderCancelled:
		var evt OrderStatusChangedEvent
		if err := json.Unmarshal(env.Payload, &evt); err != nil {
			// OrderCreated has a different payload shape — not an error
			return nil
		}
		// Invalidate order cache key if we were caching orders
		key := fmt.Sprintf("order:%s", evt.OrderID)
		return h.cache.Delete(ctx, key)

	case EventUserRegistered:
		var evt UserRegisteredEvent
		if err := json.Unmarshal(env.Payload, &evt); err != nil {
			return err
		}
		// Invalidate any user-level cache
		key := fmt.Sprintf("user:%s", evt.UserID)
		return h.cache.Delete(ctx, key)
	}
	return nil
}

// ─── Notification Service (stub) ─────────────────────────────────────────────

type NotificationHandler struct {
	log *zap.Logger
}

func NewNotificationHandler(log *zap.Logger) *NotificationHandler {
	return &NotificationHandler{log: log}
}

func (h *NotificationHandler) Handle(ctx context.Context, env *Envelope) error {
	h.log.Info("notification triggered",
		zap.String("event_type", string(env.EventType)),
		zap.String("aggregate_id", env.AggregateID),
		zap.String("correlation_id", env.CorrelationID))
	// In production: send email via SES, push via FCM, SMS via Twilio
	return nil
}

// ─── Analytics Service (stub) ────────────────────────────────────────────────

type AnalyticsHandler struct {
	log *zap.Logger
}

func NewAnalyticsHandler(log *zap.Logger) *AnalyticsHandler {
	return &AnalyticsHandler{log: log}
}

func (h *AnalyticsHandler) Handle(ctx context.Context, env *Envelope) error {
	h.log.Info("analytics event received",
		zap.String("event_type", string(env.EventType)),
		zap.String("aggregate_id", env.AggregateID),
		zap.Time("occurred_at", env.OccurredAt))
	// In production: write to ClickHouse, BigQuery, or Redshift
	return nil
}

// ─── DLQ Reprocessor ─────────────────────────────────────────────────────────

// DLQReprocessorHandler reads from the dead-letter queue and attempts reprocessing.
// In production this would have a manual approval step or exponential back-off.
type DLQReprocessorHandler struct {
	producer *Producer
	log      *zap.Logger
}

func NewDLQReprocessorHandler(producer *Producer, log *zap.Logger) *DLQReprocessorHandler {
	return &DLQReprocessorHandler{producer: producer, log: log}
}

func (h *DLQReprocessorHandler) Handle(ctx context.Context, env *Envelope) error {
	h.log.Warn("DLQ event received — manual intervention may be required",
		zap.String("event_type", string(env.EventType)),
		zap.String("aggregate_id", env.AggregateID),
		zap.String("event_id", env.EventID))
	// In production: alert on-call, write to incident tracker, retry with back-off
	return nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────
