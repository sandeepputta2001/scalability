// Bulkhead Pattern
// ─────────────────
// Isolates resource pools so that a failure or overload in one component
// cannot exhaust resources needed by other components.
//
// Named after ship bulkheads: watertight compartments that prevent a single
// hull breach from sinking the entire ship.
//
// Problem without bulkheads:
//
//	All operations share one goroutine pool / connection pool.
//	If the payment service is slow, it fills the shared pool.
//	Order queries, product reads, and cart operations all start timing out.
//	One slow dependency cascades into a full system outage.
//
// Solution with bulkheads:
//
//	Each critical operation type gets its own semaphore (concurrency limit).
//	Payment calls: max 20 concurrent
//	DB reads:      max 100 concurrent
//	Cache reads:   max 200 concurrent
//	If payment is slow and fills its 20 slots, DB reads are unaffected.
//
// Tradeoff:
//
//	✓ Fault isolation: one slow dependency can't starve others
//	✓ Predictable resource usage per operation type
//	✗ Under-utilisation: payment bulkhead may be idle while DB bulkhead is full
//	✗ Requires tuning: wrong limits cause unnecessary rejections
package patterns

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Bulkhead limits concurrent executions of a specific operation type.
type Bulkhead struct {
	name     string
	sem      chan struct{} // semaphore: buffered channel
	timeout  time.Duration
	rejected atomic.Int64
	acquired atomic.Int64
	log      *zap.Logger
}

// NewBulkhead creates a bulkhead with the given concurrency limit.
// timeout: how long to wait for a slot before rejecting the request.
// 0 = fail immediately if no slot available (non-blocking).
func NewBulkhead(name string, maxConcurrent int, timeout time.Duration, log *zap.Logger) *Bulkhead {
	return &Bulkhead{
		name:    name,
		sem:     make(chan struct{}, maxConcurrent),
		timeout: timeout,
		log:     log,
	}
}

// Do executes fn within the bulkhead.
// Returns ErrBulkheadFull if the concurrency limit is reached.
func (b *Bulkhead) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	// Try to acquire a slot
	if b.timeout == 0 {
		// Non-blocking: fail immediately if full
		select {
		case b.sem <- struct{}{}:
		default:
			b.rejected.Add(1)
			b.log.Warn("bulkhead full, rejecting request",
				zap.String("name", b.name),
				zap.Int64("rejected_total", b.rejected.Load()))
			return fmt.Errorf("%w: %s", ErrBulkheadFull, b.name)
		}
	} else {
		// Wait up to timeout for a slot
		timer := time.NewTimer(b.timeout)
		defer timer.Stop()
		select {
		case b.sem <- struct{}{}:
		case <-timer.C:
			b.rejected.Add(1)
			return fmt.Errorf("%w: %s (timeout after %v)", ErrBulkheadFull, b.name, b.timeout)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	b.acquired.Add(1)
	defer func() {
		<-b.sem // release slot
	}()

	return fn(ctx)
}

// Stats returns current bulkhead metrics.
func (b *Bulkhead) Stats() BulkheadStats {
	return BulkheadStats{
		Name:          b.name,
		Capacity:      cap(b.sem),
		InUse:         len(b.sem),
		Available:     cap(b.sem) - len(b.sem),
		TotalAcquired: b.acquired.Load(),
		TotalRejected: b.rejected.Load(),
	}
}

// BulkheadStats is the observability payload.
type BulkheadStats struct {
	Name          string `json:"name"`
	Capacity      int    `json:"capacity"`
	InUse         int    `json:"in_use"`
	Available     int    `json:"available"`
	TotalAcquired int64  `json:"total_acquired"`
	TotalRejected int64  `json:"total_rejected"`
}

var ErrBulkheadFull = fmt.Errorf("bulkhead capacity exceeded")

// ─── BulkheadRegistry ────────────────────────────────────────────────────────

// BulkheadRegistry holds all named bulkheads for the application.
// Centralised so the admin endpoint can report all stats in one call.
type BulkheadRegistry struct {
	bulkheads map[string]*Bulkhead
}

func NewBulkheadRegistry() *BulkheadRegistry {
	return &BulkheadRegistry{bulkheads: make(map[string]*Bulkhead)}
}

func (r *BulkheadRegistry) Register(b *Bulkhead) {
	r.bulkheads[b.name] = b
}

func (r *BulkheadRegistry) Get(name string) (*Bulkhead, bool) {
	b, ok := r.bulkheads[name]
	return b, ok
}

func (r *BulkheadRegistry) AllStats() []BulkheadStats {
	stats := make([]BulkheadStats, 0, len(r.bulkheads))
	for _, b := range r.bulkheads {
		stats = append(stats, b.Stats())
	}
	return stats
}
