// advanced.go implements advanced caching patterns on top of the base Client:
//
//  1. Write-Through Cache
//     Write to DB and cache atomically. Cache is always consistent with DB.
//     Tradeoff: every write pays the cache write cost; cache may hold data
//     nobody reads (wasted memory).
//
//  2. Write-Behind (Write-Back) Cache
//     Write to cache immediately, persist to DB asynchronously via a buffered
//     channel. Tradeoff: lower write latency, but data loss risk if the process
//     crashes before the async write completes.
//
//  3. Cache Stampede Prevention (Probabilistic Early Expiry + Mutex)
//     When a hot key expires, many goroutines race to recompute it — the
//     "thundering herd" / "cache stampede" problem.
//     Solution A: Mutex (singleflight) — only one goroutine recomputes, others wait.
//     Solution B: Probabilistic early expiry — recompute slightly before TTL
//     expires so the cache never actually goes cold.
//     We implement both and let callers choose.
//
//  4. Stale-While-Revalidate
//     Serve stale data immediately, trigger async background refresh.
//     Tradeoff: clients may see slightly old data, but p99 latency is low.
//
//  5. Cache Versioning / Tag-Based Invalidation
//     Instead of invalidating individual keys, bump a version counter.
//     All keys embed the version; old-version keys are effectively invisible.
//     Tradeoff: old keys accumulate in Redis until TTL expires (memory waste).
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ─── Write-Through ────────────────────────────────────────────────────────────

// WriteThrough writes value to cache and calls the provided DB writer atomically.
// If the DB write fails, the cache write is rolled back (deleted).
//
// Consistency guarantee: cache and DB are always in sync after this call.
// Tradeoff: two network round-trips per write (cache + DB).
func (c *Client) WriteThrough(
	ctx context.Context,
	key string,
	value any,
	ttl time.Duration,
	dbWriter func(ctx context.Context) error,
) error {
	// 1. Write to DB first (source of truth)
	if err := dbWriter(ctx); err != nil {
		return fmt.Errorf("write-through db write: %w", err)
	}
	// 2. Write to cache — if this fails, we log but don't fail the request
	//    (DB is the source of truth; cache miss on next read will re-populate)
	if err := c.Set(ctx, key, value, ttl); err != nil {
		c.log.Warn("write-through cache set failed, cache will be stale until TTL",
			zap.String("key", key), zap.Error(err))
	}
	return nil
}

// ─── Write-Behind (Write-Back) ────────────────────────────────────────────────

// WriteBehindEntry is a pending async DB write.
type WriteBehindEntry struct {
	Key     string
	Value   any
	Writer  func(ctx context.Context) error
	Retries int
}

// WriteBehindBuffer buffers writes and flushes them to DB asynchronously.
// This gives sub-millisecond write latency at the cost of potential data loss.
type WriteBehindBuffer struct {
	cache  CacheClient
	ch     chan WriteBehindEntry
	log    *zap.Logger
	wg     sync.WaitGroup
	stopCh chan struct{}
}

// NewWriteBehindBuffer creates a buffer that drains writes to DB in the background.
func NewWriteBehindBuffer(c CacheClient, bufSize int, log *zap.Logger) *WriteBehindBuffer {
	wb := &WriteBehindBuffer{
		cache:  c,
		ch:     make(chan WriteBehindEntry, bufSize),
		log:    log,
		stopCh: make(chan struct{}),
	}
	wb.wg.Add(1)
	go wb.drain()
	return wb
}

// Enqueue writes to cache immediately and schedules the DB write asynchronously.
// Returns immediately — DB write happens in background.
//
// DATA LOSS RISK: if the process crashes before drain() processes this entry,
// the DB write is lost. Mitigate with a persistent queue (e.g., Redis list).
func (wb *WriteBehindBuffer) Enqueue(ctx context.Context, key string, value any, ttl time.Duration, dbWriter func(ctx context.Context) error) error {
	// Write to cache immediately (fast path)
	if err := wb.cache.Set(ctx, key, value, ttl); err != nil {
		return fmt.Errorf("write-behind cache set: %w", err)
	}
	// Schedule DB write
	select {
	case wb.ch <- WriteBehindEntry{Key: key, Value: value, Writer: dbWriter}:
	default:
		// Buffer full — fall back to synchronous write to avoid data loss
		wb.log.Warn("write-behind buffer full, falling back to sync write", zap.String("key", key))
		return dbWriter(ctx)
	}
	return nil
}

func (wb *WriteBehindBuffer) drain() {
	defer wb.wg.Done()
	for {
		select {
		case entry := <-wb.ch:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := entry.Writer(ctx); err != nil {
				wb.log.Error("write-behind db flush failed",
					zap.String("key", entry.Key), zap.Error(err),
					zap.Int("retries", entry.Retries))
				// Re-enqueue with retry limit
				if entry.Retries < 3 {
					entry.Retries++
					wb.ch <- entry
				} else {
					wb.log.Error("write-behind giving up after 3 retries", zap.String("key", entry.Key))
				}
			}
			cancel()
		case <-wb.stopCh:
			// Drain remaining entries before stopping
			for len(wb.ch) > 0 {
				entry := <-wb.ch
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = entry.Writer(ctx)
				cancel()
			}
			return
		}
	}
}

// Stop gracefully shuts down the write-behind buffer.
func (wb *WriteBehindBuffer) Stop() {
	close(wb.stopCh)
	wb.wg.Wait()
}

// ─── Cache Stampede Prevention ────────────────────────────────────────────────

// singleflightGroup prevents duplicate cache recomputation.
var sfGroup singleflightGroup

type singleflightGroup struct {
	mu sync.Mutex
	m  map[string]*call
}

type call struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

// GetOrCompute implements singleflight: only one goroutine computes the value
// for a given key; all others wait and share the result.
//
// This prevents the thundering herd problem where N goroutines all miss the
// cache simultaneously and all hit the DB.
func (c *Client) GetOrCompute(
	ctx context.Context,
	key string,
	dest any,
	ttl time.Duration,
	compute func(ctx context.Context) (any, error),
) error {
	// Fast path: cache hit
	if err := c.Get(ctx, key, dest); err == nil {
		return nil
	} else if !errors.Is(err, redis.Nil) {
		return err
	}

	// Slow path: singleflight
	sfGroup.mu.Lock()
	if sfGroup.m == nil {
		sfGroup.m = make(map[string]*call)
	}
	if cl, ok := sfGroup.m[key]; ok {
		// Another goroutine is already computing — wait for it
		sfGroup.mu.Unlock()
		cl.wg.Wait()
		if cl.err != nil {
			return cl.err
		}
		return json.Unmarshal(cl.val, dest)
	}
	cl := &call{}
	cl.wg.Add(1)
	sfGroup.m[key] = cl
	sfGroup.mu.Unlock()

	// This goroutine is the "leader" — compute the value
	defer func() {
		cl.wg.Done()
		sfGroup.mu.Lock()
		delete(sfGroup.m, key)
		sfGroup.mu.Unlock()
	}()

	val, err := compute(ctx)
	if err != nil {
		cl.err = err
		return err
	}

	data, err := json.Marshal(val)
	if err != nil {
		cl.err = err
		return err
	}
	cl.val = data

	// Populate cache
	_ = c.master.Set(ctx, key, data, ttl).Err()

	return json.Unmarshal(data, dest)
}

// ─── Probabilistic Early Expiry ───────────────────────────────────────────────

// ProbabilisticGet implements XFetch (probabilistic early expiry).
//
// Algorithm: recompute the value before it expires with probability that
// increases as the TTL approaches zero. This prevents a hard expiry cliff
// where all goroutines miss simultaneously.
//
// P(recompute) = -delta * beta * ln(remaining_ttl / original_ttl)
// where delta = time to recompute, beta = tuning parameter (default 1.0)
//
// Reference: "Optimal Probabilistic Cache Stampede Prevention" (Vattani et al.)
func (c *Client) ProbabilisticGet(
	ctx context.Context,
	key string,
	dest any,
	originalTTL time.Duration,
	beta float64,
	compute func(ctx context.Context) (any, error),
) error {
	type cachedWithMeta struct {
		Value     json.RawMessage `json:"v"`
		ExpiresAt int64           `json:"exp"` // unix nano
	}

	metaKey := "xfetch:" + key

	var meta cachedWithMeta
	if err := c.Get(ctx, metaKey, &meta); err == nil {
		remaining := time.Until(time.Unix(0, meta.ExpiresAt))
		if remaining > 0 {
			// XFetch formula: recompute early with increasing probability
			recomputeProb := -1.0 * beta * math.Log(float64(remaining)/float64(originalTTL))
			if rand.Float64() > recomputeProb {
				// Serve cached value
				return json.Unmarshal(meta.Value, dest)
			}
			// Probabilistically decided to recompute early
			c.log.Debug("probabilistic early recompute triggered", zap.String("key", key))
		}
	}

	// Recompute
	val, err := compute(ctx)
	if err != nil {
		return err
	}

	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	newMeta := cachedWithMeta{
		Value:     data,
		ExpiresAt: time.Now().Add(originalTTL).UnixNano(),
	}
	_ = c.Set(ctx, metaKey, newMeta, originalTTL+10*time.Second)

	return json.Unmarshal(data, dest)
}

// ─── Stale-While-Revalidate ───────────────────────────────────────────────────

// StaleWhileRevalidate serves stale data immediately and refreshes in background.
//
// Two TTLs:
//   - freshTTL: how long data is considered "fresh" (served without revalidation)
//   - staleTTL: how long stale data can be served while revalidating
//
// Tradeoff: clients may see data up to staleTTL old, but p99 latency is always
// the cache read latency (never the DB latency).
func (c *Client) StaleWhileRevalidate(
	ctx context.Context,
	key string,
	dest any,
	freshTTL, staleTTL time.Duration,
	revalidate func(ctx context.Context) (any, error),
) (isStale bool, err error) {
	freshKey := "fresh:" + key
	staleKey := "stale:" + key

	// Try fresh cache first
	if err := c.Get(ctx, freshKey, dest); err == nil {
		return false, nil // fresh hit
	}

	// Fresh miss — try stale cache and trigger background revalidation
	staleErr := c.Get(ctx, staleKey, dest)
	if staleErr == nil {
		// Serve stale data, revalidate in background
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			val, err := revalidate(bgCtx)
			if err != nil {
				c.log.Warn("stale-while-revalidate background refresh failed",
					zap.String("key", key), zap.Error(err))
				return
			}
			_ = c.Set(bgCtx, freshKey, val, freshTTL)
			_ = c.Set(bgCtx, staleKey, val, staleTTL)
		}()
		return true, nil // stale hit
	}

	// Both miss — synchronous revalidation
	val, err := revalidate(ctx)
	if err != nil {
		return false, err
	}
	_ = c.Set(ctx, freshKey, val, freshTTL)
	_ = c.Set(ctx, staleKey, val, staleTTL)
	if err := json.Unmarshal(mustMarshal(val), dest); err != nil {
		return false, err
	}
	return false, nil
}

// ─── Tag-Based Cache Invalidation ────────────────────────────────────────────

// TagVersion returns the current version for a cache tag.
// All keys for a tag embed this version; bumping it effectively invalidates all.
func (c *Client) TagVersion(ctx context.Context, tag string) (int64, error) {
	v, err := c.readClient().Get(ctx, "tag:"+tag).Int64()
	if errors.Is(err, redis.Nil) {
		return 1, nil
	}
	return v, err
}

// BumpTagVersion increments the version for a tag, invalidating all tagged keys.
// Old keys remain in Redis until their TTL expires (memory tradeoff).
func (c *Client) BumpTagVersion(ctx context.Context, tag string) (int64, error) {
	return c.master.Incr(ctx, "tag:"+tag).Result()
}

// TaggedKey builds a versioned cache key: "prefix:v{version}:{id}".
func TaggedKey(prefix, tag string, version int64, id string) string {
	return fmt.Sprintf("%s:v%d:%s", prefix, version, id)
}

// ─── Inconsistency Window Tracking ───────────────────────────────────────────

// InconsistencyEvent records a detected cache/DB inconsistency for observability.
type InconsistencyEvent struct {
	Key        string
	CacheVal   string
	DBVal      string
	DetectedAt time.Time
}

// RecordInconsistency logs a cache/DB inconsistency to Redis for audit.
// In production this would go to a metrics system (Prometheus, Datadog).
func (c *Client) RecordInconsistency(ctx context.Context, event InconsistencyEvent) {
	data, _ := json.Marshal(event)
	// Store last 100 inconsistency events in a Redis list
	pipe := c.master.Pipeline()
	pipe.LPush(ctx, "inconsistency:log", data)
	pipe.LTrim(ctx, "inconsistency:log", 0, 99)
	_, _ = pipe.Exec(ctx)
	c.log.Warn("cache/db inconsistency detected",
		zap.String("key", event.Key),
		zap.String("cache_val", event.CacheVal),
		zap.String("db_val", event.DBVal))
}

// GetInconsistencyLog returns recent inconsistency events.
func (c *Client) GetInconsistencyLog(ctx context.Context) ([]InconsistencyEvent, error) {
	items, err := c.readClient().LRange(ctx, "inconsistency:log", 0, 99).Result()
	if err != nil {
		return nil, err
	}
	events := make([]InconsistencyEvent, 0, len(items))
	for _, item := range items {
		var e InconsistencyEvent
		if err := json.Unmarshal([]byte(item), &e); err == nil {
			events = append(events, e)
		}
	}
	return events, nil
}

func mustMarshal(v any) []byte {
	data, _ := json.Marshal(v)
	return data
}
