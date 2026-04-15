// CQRS — Command Query Responsibility Segregation
// ─────────────────────────────────────────────────
// Separates the write model (Commands) from the read model (Queries).
//
// Why CQRS in a distributed system?
//   - Write path: strong consistency, goes to primary DB, triggers events
//   - Read path: eventual consistency, goes to replicas/cache, optimised for
//     specific query shapes (denormalised projections)
//
// Without CQRS: one model serves both reads and writes.
//
//	Problem: write model is normalised (good for integrity), but read model
//	needs denormalised data (good for performance). Trying to serve both
//	from one model leads to N+1 queries, complex joins, or over-fetching.
//
// With CQRS:
//
//	Commands: CreateOrder, UpdateOrderStatus, RegisterUser
//	  → validated, authorised, written to primary DB
//	  → emit domain events (via Kafka / outbox)
//	Queries: GetOrderWithUser, ListOrdersByStatus, GetProductWithInventory
//	  → read from replicas/cache
//	  → return denormalised view models (no joins needed)
//
// Tradeoff:
//
//	✓ Read and write models optimised independently
//	✓ Read replicas can serve queries without touching write primary
//	✗ Eventual consistency between write and read models
//	✗ More code: separate command handlers and query handlers
//	✗ Projection maintenance: read models must be updated when events arrive
package patterns

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ─── Command Bus ──────────────────────────────────────────────────────────────

// Command is a request to change system state.
// Commands are validated before execution and may be rejected.
type Command interface {
	CommandName() string
	Validate() error
}

// CommandHandler executes a command and returns a result.
type CommandHandler[C Command, R any] interface {
	Handle(ctx context.Context, cmd C) (R, error)
}

// CommandBus routes commands to their handlers.
type CommandBus struct {
	handlers map[string]func(ctx context.Context, cmd Command) (any, error)
}

func NewCommandBus() *CommandBus {
	return &CommandBus{handlers: make(map[string]func(ctx context.Context, cmd Command) (any, error))}
}

// Register maps a command name to a handler function.
func (b *CommandBus) Register(name string, handler func(ctx context.Context, cmd Command) (any, error)) {
	b.handlers[name] = handler
}

// Dispatch validates and executes a command.
func (b *CommandBus) Dispatch(ctx context.Context, cmd Command) (any, error) {
	if err := cmd.Validate(); err != nil {
		return nil, fmt.Errorf("command validation failed: %w", err)
	}
	h, ok := b.handlers[cmd.CommandName()]
	if !ok {
		return nil, fmt.Errorf("no handler registered for command: %s", cmd.CommandName())
	}
	return h(ctx, cmd)
}

// ─── Query Bus ────────────────────────────────────────────────────────────────

// Query is a request to read system state. Queries never change state.
type Query interface {
	QueryName() string
}

// QueryBus routes queries to their handlers.
type QueryBus struct {
	handlers map[string]func(ctx context.Context, q Query) (any, error)
}

func NewQueryBus() *QueryBus {
	return &QueryBus{handlers: make(map[string]func(ctx context.Context, q Query) (any, error))}
}

func (b *QueryBus) Register(name string, handler func(ctx context.Context, q Query) (any, error)) {
	b.handlers[name] = handler
}

func (b *QueryBus) Execute(ctx context.Context, q Query) (any, error) {
	h, ok := b.handlers[q.QueryName()]
	if !ok {
		return nil, fmt.Errorf("no handler registered for query: %s", q.QueryName())
	}
	return h(ctx, q)
}

// ─── Commands ─────────────────────────────────────────────────────────────────

// CreateOrderCommand is the write-side command for placing an order.
type CreateOrderCommand struct {
	UserID        uuid.UUID
	Items         []OrderItemSpec
	CorrelationID string
}

func (c CreateOrderCommand) CommandName() string { return "CreateOrder" }
func (c CreateOrderCommand) Validate() error {
	if c.UserID == uuid.Nil {
		return fmt.Errorf("userID is required")
	}
	if len(c.Items) == 0 {
		return fmt.Errorf("at least one item is required")
	}
	for _, item := range c.Items {
		if item.Quantity <= 0 {
			return fmt.Errorf("item %s: quantity must be > 0", item.ProductID)
		}
	}
	return nil
}

// UpdateOrderStatusCommand changes an order's status.
type UpdateOrderStatusCommand struct {
	OrderID uuid.UUID
	UserID  uuid.UUID
	Status  string
}

func (c UpdateOrderStatusCommand) CommandName() string { return "UpdateOrderStatus" }
func (c UpdateOrderStatusCommand) Validate() error {
	if c.OrderID == uuid.Nil {
		return fmt.Errorf("orderID is required")
	}
	validStatuses := map[string]bool{
		"pending": true, "confirmed": true, "shipped": true,
		"delivered": true, "cancelled": true,
	}
	if !validStatuses[c.Status] {
		return fmt.Errorf("invalid status: %s", c.Status)
	}
	return nil
}

// ─── Queries ──────────────────────────────────────────────────────────────────

// GetOrderQuery fetches a single order with its items.
type GetOrderQuery struct {
	OrderID uuid.UUID
	UserID  uuid.UUID
}

func (q GetOrderQuery) QueryName() string { return "GetOrder" }

// ListOrdersByUserQuery fetches all orders for a user.
type ListOrdersByUserQuery struct {
	UserID   uuid.UUID
	Page     int
	PageSize int
}

func (q ListOrdersByUserQuery) QueryName() string { return "ListOrdersByUser" }

// GetProductQuery fetches a product with optional consistency level.
type GetProductQuery struct {
	ProductID   string
	Consistency string // "strong" | "eventual" | "stale"
}

func (q GetProductQuery) QueryName() string { return "GetProduct" }

// ─── Read Models (Projections) ────────────────────────────────────────────────

// OrderReadModel is the denormalised read-side view of an order.
// It includes user info (from a different shard) pre-joined.
// This is updated by the projection handler when order events arrive.
type OrderReadModel struct {
	OrderID    uuid.UUID `json:"order_id"`
	UserID     uuid.UUID `json:"user_id"`
	UserEmail  string    `json:"user_email"`
	UserName   string    `json:"user_name"`
	Status     string    `json:"status"`
	TotalPrice float64   `json:"total_price"`
	ItemCount  int       `json:"item_count"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ProductReadModel is the denormalised read-side view of a product.
type ProductReadModel struct {
	ProductID string    `json:"product_id"`
	Name      string    `json:"name"`
	Price     float64   `json:"price"`
	Stock     int       `json:"stock"`
	Category  string    `json:"category"`
	Tags      []string  `json:"tags"`
	UpdatedAt time.Time `json:"updated_at"`
	// Computed fields for the read model
	InStock    bool   `json:"in_stock"`
	StockLevel string `json:"stock_level"` // "high" | "medium" | "low" | "out"
}

// NewProductReadModel builds a read model from a raw product.
func NewProductReadModel(id, name, category string, price float64, stock int, tags []string, updatedAt time.Time) ProductReadModel {
	level := "out"
	switch {
	case stock > 100:
		level = "high"
	case stock > 20:
		level = "medium"
	case stock > 0:
		level = "low"
	}
	return ProductReadModel{
		ProductID:  id,
		Name:       name,
		Price:      price,
		Stock:      stock,
		Category:   category,
		Tags:       tags,
		UpdatedAt:  updatedAt,
		InStock:    stock > 0,
		StockLevel: level,
	}
}
