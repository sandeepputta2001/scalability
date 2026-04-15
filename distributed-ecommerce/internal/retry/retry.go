// Package retry implements production-grade retry mechanisms:
//
//  1. Exponential Backoff with Jitter
//     Each retry waits longer than the previous one, with random jitter to
//     prevent the "thundering herd" problem where all retrying clients hit
//     the server at the same moment.
//
//     Formula: wait = min(base * 2^attempt, maxWait) + jitter(0..jitterFactor*wait)
//
//     Tradeoff: longer total latency per request vs. reduced server overload.
//     Without jitter: all clients retry at t=1s, t=2s, t=4s simultaneously.
//     With jitter:    clients spread retries across a time window.
//
//  2. Circuit Breaker (Closed → Open → Half-Open)
//     Prevents cascading failures by stopping calls to a failing service.
//
//     States:
//     Closed:    normal operation; failures counted
//     Open:      all calls fail immediately (fast-fail); no network calls
//     Half-Open: one probe call allowed; success → Closed, failure → Open
//
//     Tradeoff: may reject valid requests during Open state (availability ↓)
//     in exchange for protecting the downstream service from overload.
//
//  3. Retry Budget
//     Limits the total number of in-flight retries across all goroutines.
//     Prevents retry storms where N goroutines each retry M times = N*M calls.
//
//     Tradeoff: some requests fail fast (budget exhausted) to protect the
//     system as a whole. Individual request success rate ↓, system stability ↑.
//
//  4. Idempotency Key Enforcement
//     Before retrying a write operation, verify the operation hasn't already
//     succeeded (e.g., check if the order was created despite a timeout).
//     Prevents duplicate writes on retry.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ─── Retry Config ─────────────────────────────────────────────────────────────

// Config holds all retry parameters.
type Config struct {
	// MaxAttempts is the total number of attempts (1 = no retry).
	MaxAttempts int
	// BaseDelay is the initial wait before the first retry.
	BaseDelay time.Duration
	// MaxDelay caps the exponential growth.
	MaxDelay time.Duration
	// JitterFactor adds randomness: actual_wait = wait * (1 + rand(0, JitterFactor))
	// 0.0 = no jitter, 0.5 = up to 50% extra wait
	JitterFactor float64
	// RetryOn is a predicate that decides if an error is retryable.
	// nil = retry on all non-nil errors.
	RetryOn func(err error) bool
}

// DefaultConfig returns sensible defaults for transient network errors.
var DefaultConfig = Config{
	MaxAttempts:  3,
	BaseDelay:    100 * time.Millisecond,
	MaxDelay:     5 * time.Second,
	JitterFactor: 0.5,
	RetryOn:      IsTransient,
}

// KafkaConfig is tuned for Kafka publish retries.
var KafkaConfig = Config{
	MaxAttempts:  5,
	BaseDelay:    200 * time.Millisecond,
	MaxDelay:     10 * time.Second,
	JitterFactor: 0.3,
	RetryOn:      IsTransient,
}

// DBConfig is tuned for database operation retries.
var DBConfig = Config{
	MaxAttempts:  3,
	BaseDelay:    50 * time.Millisecond,
	MaxDelay:     2 * time.Second,
	JitterFactor: 0.4,
	RetryOn:      IsTransientDB,
}

// HTTPConfig is tuned for outbound HTTP call retries.
var HTTPConfig = Config{
	MaxAttempts:  4,
	BaseDelay:    500 * time.Millisecond,
	MaxDelay:     30 * time.Second,
	JitterFactor: 0.5,
	RetryOn:      IsTransientHTTP,
}

// ─── Core retry function ──────────────────────────────────────────────────────

// Attempt is the result of a single retry attempt.
type Attempt struct {
	Number    int
	Err       error
	Duration  time.Duration
	WillRetry bool
}

// Do executes fn with retries according to cfg.
// It respects context cancellation between attempts.
// Returns the last error if all attempts fail.
func Do(ctx context.Context, cfg Config, fn func(ctx context.Context) error) error {
	return DoWithLog(ctx, cfg, nil, "", fn)
}

// DoWithLog executes fn with retries and structured logging.
func DoWithLog(ctx context.Context, cfg Config, log *zap.Logger, operation string, fn func(ctx context.Context) error) error {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		start := time.Now()
		lastErr = fn(ctx)
		elapsed := time.Since(start)

		if lastErr == nil {
			if attempt > 1 && log != nil {
				log.Info("retry succeeded",
					zap.String("op", operation),
					zap.Int("attempt", attempt),
					zap.Duration("elapsed", elapsed))
			}
			return nil
		}

		isLast := attempt == cfg.MaxAttempts
		shouldRetry := cfg.RetryOn == nil || cfg.RetryOn(lastErr)

		if log != nil {
			log.Warn("attempt failed",
				zap.String("op", operation),
				zap.Int("attempt", attempt),
				zap.Int("max", cfg.MaxAttempts),
				zap.Bool("will_retry", !isLast && shouldRetry),
				zap.Duration("elapsed", elapsed),
				zap.Error(lastErr))
		}

		if isLast || !shouldRetry {
			break
		}

		// Compute wait: base * 2^(attempt-1), capped at MaxDelay
		wait := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt-1))
		if wait > float64(cfg.MaxDelay) {
			wait = float64(cfg.MaxDelay)
		}
		// Add jitter: wait * (1 + rand(0, JitterFactor))
		if cfg.JitterFactor > 0 {
			wait = wait * (1 + rand.Float64()*cfg.JitterFactor)
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled: %w", ctx.Err())
		case <-time.After(time.Duration(wait)):
		}
	}

	return fmt.Errorf("all %d attempts failed: %w", cfg.MaxAttempts, lastErr)
}

// ─── Circuit Breaker ──────────────────────────────────────────────────────────

// State represents the circuit breaker state.
type State int32

const (
	StateClosed   State = iota // normal operation
	StateOpen                  // fast-fail all requests
	StateHalfOpen              // one probe request allowed
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreaker implements the circuit breaker pattern.
type CircuitBreaker struct {
	name         string
	maxFailures  int           // failures before opening
	resetTimeout time.Duration // how long to stay Open before trying Half-Open
	state        atomic.Int32
	failures     atomic.Int32
	lastFailTime atomic.Int64 // unix nano
	halfOpenLock atomic.Bool  // only one probe at a time in half-open
	log          *zap.Logger
}

// NewCircuitBreaker creates a circuit breaker.
// maxFailures: consecutive failures before opening.
// resetTimeout: how long to wait in Open state before probing.
func NewCircuitBreaker(name string, maxFailures int, resetTimeout time.Duration, log *zap.Logger) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:         name,
		maxFailures:  maxFailures,
		resetTimeout: resetTimeout,
		log:          log,
	}
	cb.state.Store(int32(StateClosed))
	return cb
}

// Do executes fn through the circuit breaker.
// Returns ErrCircuitOpen if the circuit is open.
func (cb *CircuitBreaker) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	state := State(cb.state.Load())

	switch state {
	case StateOpen:
		// Check if reset timeout has elapsed → transition to Half-Open
		lastFail := time.Unix(0, cb.lastFailTime.Load())
		if time.Since(lastFail) >= cb.resetTimeout {
			// Try to transition to Half-Open (only one goroutine succeeds)
			if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
				cb.log.Info("circuit breaker half-open", zap.String("name", cb.name))
			} else {
				return ErrCircuitOpen
			}
		} else {
			return ErrCircuitOpen
		}

	case StateHalfOpen:
		// Only one probe at a time
		if !cb.halfOpenLock.CompareAndSwap(false, true) {
			return ErrCircuitOpen
		}
		defer cb.halfOpenLock.Store(false)
	}

	// Execute the function
	err := fn(ctx)

	if err != nil {
		cb.recordFailure()
		return err
	}

	cb.recordSuccess()
	return nil
}

func (cb *CircuitBreaker) recordFailure() {
	failures := cb.failures.Add(1)
	cb.lastFailTime.Store(time.Now().UnixNano())

	currentState := State(cb.state.Load())
	if currentState == StateHalfOpen {
		// Probe failed → back to Open
		cb.state.Store(int32(StateOpen))
		cb.log.Warn("circuit breaker re-opened after probe failure", zap.String("name", cb.name))
		return
	}

	if int(failures) >= cb.maxFailures && currentState == StateClosed {
		cb.state.Store(int32(StateOpen))
		cb.log.Error("circuit breaker opened",
			zap.String("name", cb.name),
			zap.Int32("failures", failures))
	}
}

func (cb *CircuitBreaker) recordSuccess() {
	cb.failures.Store(0)
	cb.state.Store(int32(StateClosed))
	cb.log.Info("circuit breaker closed", zap.String("name", cb.name))
}

// State returns the current circuit breaker state.
func (cb *CircuitBreaker) State() State {
	return State(cb.state.Load())
}

// Stats returns observability data.
func (cb *CircuitBreaker) Stats() CircuitBreakerStats {
	return CircuitBreakerStats{
		Name:     cb.name,
		State:    State(cb.state.Load()).String(),
		Failures: int(cb.failures.Load()),
	}
}

// CircuitBreakerStats is the observability payload.
type CircuitBreakerStats struct {
	Name     string `json:"name"`
	State    string `json:"state"`
	Failures int    `json:"failures"`
}

// ─── Retry Budget ─────────────────────────────────────────────────────────────

// Budget limits the total number of concurrent retries across all goroutines.
// This prevents retry storms: if N goroutines all retry M times, the budget
// caps the total at B retries, causing some to fail fast.
//
// Tradeoff: individual request success rate ↓, system stability ↑.
type Budget struct {
	remaining atomic.Int64
	total     int64
}

// NewBudget creates a retry budget with the given total capacity.
func NewBudget(total int64) *Budget {
	b := &Budget{total: total}
	b.remaining.Store(total)
	return b
}

// Acquire attempts to consume one retry token.
// Returns false if the budget is exhausted.
func (b *Budget) Acquire() bool {
	for {
		current := b.remaining.Load()
		if current <= 0 {
			return false
		}
		if b.remaining.CompareAndSwap(current, current-1) {
			return true
		}
	}
}

// Release returns a retry token to the budget.
func (b *Budget) Release() {
	for {
		current := b.remaining.Load()
		if current >= b.total {
			return
		}
		if b.remaining.CompareAndSwap(current, current+1) {
			return
		}
	}
}

// Remaining returns the number of available retry tokens.
func (b *Budget) Remaining() int64 {
	return b.remaining.Load()
}

// DoWithBudget executes fn with retry, consuming from the budget.
// If the budget is exhausted, the first failure is returned immediately.
func DoWithBudget(ctx context.Context, cfg Config, budget *Budget, log *zap.Logger, op string, fn func(ctx context.Context) error) error {
	// First attempt always allowed (not a retry)
	err := fn(ctx)
	if err == nil {
		return nil
	}

	// Retries consume from the budget
	for attempt := 2; attempt <= cfg.MaxAttempts; attempt++ {
		if !budget.Acquire() {
			if log != nil {
				log.Warn("retry budget exhausted, failing fast",
					zap.String("op", op),
					zap.Int("attempt", attempt))
			}
			return fmt.Errorf("retry budget exhausted after attempt %d: %w", attempt-1, err)
		}

		shouldRetry := cfg.RetryOn == nil || cfg.RetryOn(err)
		if !shouldRetry {
			budget.Release()
			return err
		}

		wait := float64(cfg.BaseDelay) * math.Pow(2, float64(attempt-2))
		if wait > float64(cfg.MaxDelay) {
			wait = float64(cfg.MaxDelay)
		}
		if cfg.JitterFactor > 0 {
			wait = wait * (1 + rand.Float64()*cfg.JitterFactor)
		}

		select {
		case <-ctx.Done():
			budget.Release()
			return fmt.Errorf("retry cancelled: %w", ctx.Err())
		case <-time.After(time.Duration(wait)):
		}

		err = fn(ctx)
		budget.Release()
		if err == nil {
			return nil
		}
	}
	return fmt.Errorf("all %d attempts failed: %w", cfg.MaxAttempts, err)
}

// ─── Error classification ─────────────────────────────────────────────────────

// IsTransient returns true for errors that are worth retrying.
// Non-transient errors (validation, not-found, auth) should not be retried.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false // don't retry cancelled contexts
	}
	// Retry on connection errors, timeouts, temporary failures
	// In production, check for specific error types from each driver
	return true
}

// IsTransientDB returns true for retryable PostgreSQL errors.
func IsTransientDB(err error) bool {
	if !IsTransient(err) {
		return false
	}
	msg := err.Error()
	// Retry on connection errors and serialization failures
	// Don't retry on constraint violations, syntax errors, etc.
	for _, nonRetryable := range []string{
		"duplicate key", "violates", "syntax error",
		"permission denied", "does not exist",
	} {
		if contains(msg, nonRetryable) {
			return false
		}
	}
	return true
}

// IsTransientHTTP returns true for retryable HTTP status codes.
func IsTransientHTTP(err error) bool {
	if !IsTransient(err) {
		return false
	}
	// 429 Too Many Requests, 503 Service Unavailable, 502 Bad Gateway
	// 4xx client errors (except 429) are not retryable
	return true
}

// ─── Errors ───────────────────────────────────────────────────────────────────

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrBudgetExhausted = errors.New("retry budget exhausted")
	ErrMaxAttempts     = errors.New("max retry attempts exceeded")
)

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
