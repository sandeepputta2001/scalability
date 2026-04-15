// Observer / Event Bus Pattern
// ─────────────────────────────
// Decouples event producers from consumers within a single process.
// Producers publish events without knowing who handles them.
// Consumers subscribe to specific event types without knowing who produces them.
//
// This is the in-process complement to Kafka (which handles cross-service events).
// Use the event bus for:
//   - Triggering cache invalidation after a DB write (same process)
//   - Updating in-memory metrics counters
//   - Notifying multiple handlers of a domain event without coupling them
//
// Tradeoff vs. direct function calls:
//
//	✓ Loose coupling: producer doesn't import consumer packages
//	✓ Multiple handlers: add/remove handlers without changing producer
//	✗ Harder to trace: event flow is implicit, not explicit
//	✗ Error handling: one handler's panic can affect others (mitigated by recover)
//
// Tradeoff vs. Kafka:
//
//	✓ Zero latency (in-process, no network)
//	✓ No serialisation overhead
//	✗ Not durable: events lost if process crashes
//	✗ Not distributed: only within one process instance
package patterns

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
)

// DomainEvent is the base interface for all in-process domain events.
type DomainEvent interface {
	EventName() string
}

// Handler processes a domain event.
type Handler func(ctx context.Context, event DomainEvent) error

// EventBus is a thread-safe in-process pub/sub bus.
type EventBus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
	log      *zap.Logger
	// async: if true, handlers run in goroutines (fire-and-forget)
	// if false, handlers run synchronously (caller waits for all)
	async bool
}

// NewEventBus creates a new event bus.
// async=true: handlers run in goroutines (non-blocking publish)
// async=false: handlers run synchronously (blocking publish, errors returned)
func NewEventBus(async bool, log *zap.Logger) *EventBus {
	return &EventBus{
		handlers: make(map[string][]Handler),
		log:      log,
		async:    async,
	}
}

// Subscribe registers a handler for a specific event name.
// Multiple handlers can be registered for the same event.
func (b *EventBus) Subscribe(eventName string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventName] = append(b.handlers[eventName], handler)
	b.log.Debug("event handler registered", zap.String("event", eventName))
}

// Publish dispatches an event to all registered handlers.
// In async mode: returns immediately, handlers run in goroutines.
// In sync mode: waits for all handlers, returns first error.
func (b *EventBus) Publish(ctx context.Context, event DomainEvent) error {
	b.mu.RLock()
	handlers := b.handlers[event.EventName()]
	b.mu.RUnlock()

	if len(handlers) == 0 {
		return nil
	}

	if b.async {
		for _, h := range handlers {
			go func(handler Handler) {
				defer func() {
					if r := recover(); r != nil {
						b.log.Error("event handler panic recovered",
							zap.String("event", event.EventName()),
							zap.Any("panic", r))
					}
				}()
				if err := handler(ctx, event); err != nil {
					b.log.Warn("async event handler error",
						zap.String("event", event.EventName()), zap.Error(err))
				}
			}(h)
		}
		return nil
	}

	// Synchronous: run all handlers, collect errors
	var errs []error
	for _, h := range handlers {
		func() {
			defer func() {
				if r := recover(); r != nil {
					errs = append(errs, fmt.Errorf("handler panic: %v", r))
				}
			}()
			if err := h(ctx, event); err != nil {
				errs = append(errs, err)
			}
		}()
	}
	if len(errs) > 0 {
		return fmt.Errorf("event bus errors for %s: %v", event.EventName(), errs)
	}
	return nil
}

// ─── Domain Events ────────────────────────────────────────────────────────────

// ProductUpdatedEvent fires when a product's stock or price changes.
type ProductUpdatedEvent struct {
	ProductID string
	OldStock  int
	NewStock  int
	OldPrice  float64
	NewPrice  float64
}

func (e ProductUpdatedEvent) EventName() string { return "product.updated" }

// OrderCreatedInProcessEvent fires when an order is created (in-process).
type OrderCreatedInProcessEvent struct {
	OrderID    string
	UserID     string
	TotalPrice float64
	ShardKey   int
}

func (e OrderCreatedInProcessEvent) EventName() string { return "order.created.inprocess" }

// UserRegisteredInProcessEvent fires when a user registers (in-process).
type UserRegisteredInProcessEvent struct {
	UserID   string
	Email    string
	ShardKey int
}

func (e UserRegisteredInProcessEvent) EventName() string { return "user.registered.inprocess" }

// CacheInvalidationEvent fires when a cache key should be invalidated.
type CacheInvalidationEvent struct {
	Keys []string
}

func (e CacheInvalidationEvent) EventName() string { return "cache.invalidate" }
