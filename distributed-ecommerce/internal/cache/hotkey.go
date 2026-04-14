// hotkey.go implements hot-key detection and a local in-process shard cache.
//
// The Hot-Key Problem in Redis Cluster:
//
//	Redis Cluster distributes keys across N masters by slot.
//	If one key (e.g., a viral product) receives 100k req/s, ALL of those
//	requests hit the SAME master node — creating a hot spot that can
//	saturate that node's CPU/network while other nodes sit idle.
//
//	This is the fundamental tradeoff of hash-based sharding:
//	uniform distribution on average, but no protection against skew.
//
// Solutions implemented here:
//
//  1. Hot-Key Detection (sliding window counter)
//     Track access frequency per key. When a key exceeds the hot threshold,
//     promote it to the local shard cache.
//
//  2. Local Shard Cache (in-process memory)
//     A small in-process LRU cache that absorbs reads for hot keys.
//     Reads never reach Redis for hot keys → eliminates the hot spot.
//     Tradeoff: stale data (local cache has its own TTL, independent of Redis).
//     Tradeoff: memory pressure on the application process.
//     Tradeoff: each API instance has its own local cache → inconsistency
//     between instances until local TTL expires.
//
//  3. Key Replication across slots (write fan-out)
//     For extremely hot keys, write to multiple slot-tagged copies:
//     {product:123}:data:0, {product:123}:data:1, ..., :data:N
//     Reads pick a random copy → load spread across N slots/nodes.
//     Tradeoff: N× write amplification; invalidation must touch all copies.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Hot-Key Detector ─────────────────────────────────────────────────────────

// HotKeyEntry records access count for a single key.
type HotKeyEntry struct {
	Key   string
	Count uint64
}

// HotKeyDetector uses a sliding-window counter to identify hot keys.
type HotKeyDetector struct {
	mu       sync.RWMutex
	counts   map[string]*atomic.Uint64
	topN     int
	window   uint64 // reset after this many total ops
	totalOps atomic.Uint64
}

func NewHotKeyDetector(topN int, windowSize uint64) *HotKeyDetector {
	return &HotKeyDetector{
		counts: make(map[string]*atomic.Uint64),
		topN:   topN,
		window: windowSize,
	}
}

// Record increments the access counter for a key.
func (h *HotKeyDetector) Record(key string) {
	h.mu.RLock()
	c, ok := h.counts[key]
	h.mu.RUnlock()

	if !ok {
		h.mu.Lock()
		if _, ok = h.counts[key]; !ok {
			c = &atomic.Uint64{}
			h.counts[key] = c
		} else {
			c = h.counts[key]
		}
		h.mu.Unlock()
	}
	c.Add(1)

	// Reset window periodically to track recent hot keys, not all-time
	total := h.totalOps.Add(1)
	if total%h.window == 0 {
		h.reset()
	}
}

// IsHot returns true if the key's access count exceeds the threshold.
func (h *HotKeyDetector) IsHot(key string, threshold uint64) bool {
	h.mu.RLock()
	c, ok := h.counts[key]
	h.mu.RUnlock()
	return ok && c.Load() >= threshold
}

// TopN returns the N most accessed keys in the current window.
func (h *HotKeyDetector) TopN(n int) []HotKeyEntry {
	h.mu.RLock()
	entries := make([]HotKeyEntry, 0, len(h.counts))
	for k, c := range h.counts {
		entries = append(entries, HotKeyEntry{Key: k, Count: c.Load()})
	}
	h.mu.RUnlock()

	// Simple selection sort for top-N (N is small)
	for i := 0; i < n && i < len(entries); i++ {
		maxIdx := i
		for j := i + 1; j < len(entries); j++ {
			if entries[j].Count > entries[maxIdx].Count {
				maxIdx = j
			}
		}
		entries[i], entries[maxIdx] = entries[maxIdx], entries[i]
	}
	if n > len(entries) {
		n = len(entries)
	}
	return entries[:n]
}

func (h *HotKeyDetector) reset() {
	h.mu.Lock()
	h.counts = make(map[string]*atomic.Uint64)
	h.mu.Unlock()
}

// ─── Local Shard Cache (in-process LRU) ──────────────────────────────────────

// localEntry is a single cached item with expiry.
type localEntry struct {
	data      []byte
	expiresAt time.Time
}

// LocalShardCache is a small in-process cache that absorbs hot-key reads.
// It sits in front of Redis and is checked first on every Get.
//
// Consistency tradeoff:
//
//	Local cache TTL is independent of Redis TTL.
//	If Redis is updated (e.g., stock changes), the local cache will serve
//	stale data until its TTL expires (default: 5 seconds for hot keys).
//	This is acceptable for product listings but NOT for checkout/payment.
type LocalShardCache struct {
	mu      sync.RWMutex
	entries map[string]*localEntry
	maxSize int
	ttl     time.Duration
}

func NewLocalShardCache(maxSize int, ttl time.Duration) *LocalShardCache {
	lsc := &LocalShardCache{
		entries: make(map[string]*localEntry, maxSize),
		maxSize: maxSize,
		ttl:     ttl,
	}
	go lsc.evictLoop()
	return lsc
}

func (l *LocalShardCache) Set(key string, data []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// Simple eviction: if at capacity, remove a random entry
	if len(l.entries) >= l.maxSize {
		for k := range l.entries {
			delete(l.entries, k)
			break
		}
	}
	l.entries[key] = &localEntry{data: data, expiresAt: time.Now().Add(l.ttl)}
}

func (l *LocalShardCache) Get(key string) ([]byte, bool) {
	l.mu.RLock()
	e, ok := l.entries[key]
	l.mu.RUnlock()
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.data, true
}

func (l *LocalShardCache) Delete(key string) {
	l.mu.Lock()
	delete(l.entries, key)
	l.mu.Unlock()
}

func (l *LocalShardCache) evictLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		l.mu.Lock()
		for k, e := range l.entries {
			if now.After(e.expiresAt) {
				delete(l.entries, k)
			}
		}
		l.mu.Unlock()
	}
}

// ─── Hot-Key Sharded Client (wraps ClusterClient) ────────────────────────────

// HotKeyShardedClient wraps ClusterClient with:
//  1. Local shard cache for hot keys (absorbs reads, avoids Redis hot spot)
//  2. Key fan-out for write-heavy hot keys (spread writes across N slot copies)
type HotKeyShardedClient struct {
	cluster    *ClusterClient
	localCache *LocalShardCache
	hotKeys    *HotKeyDetector
	hotThresh  uint64 // access count to be considered "hot"
	fanOutN    int    // number of slot copies for fan-out writes
	log        interface{ Warn(string, ...interface{}) }
}

func NewHotKeyShardedClient(cluster *ClusterClient, hotThresh uint64, fanOutN int) *HotKeyShardedClient {
	return &HotKeyShardedClient{
		cluster:    cluster,
		localCache: NewLocalShardCache(500, 5*time.Second), // 500 hot keys, 5s local TTL
		hotKeys:    NewHotKeyDetector(100, 10000),
		hotThresh:  hotThresh,
		fanOutN:    fanOutN,
	}
}

// GetWithHotKeyProtection checks local cache first, then Redis.
// If the key is hot, it's promoted to local cache after the first Redis read.
func (h *HotKeyShardedClient) GetWithHotKeyProtection(ctx context.Context, key string, dest any) (fromLocal bool, err error) {
	h.hotKeys.Record(key)

	// 1. Check local shard cache (zero network cost)
	if data, ok := h.localCache.Get(key); ok {
		if jsonErr := json.Unmarshal(data, dest); jsonErr == nil {
			return true, nil
		}
	}

	// 2. Read from Redis cluster
	if err := h.cluster.Get(ctx, key, dest); err != nil {
		return false, err
	}

	// 3. If hot, promote to local cache
	if h.hotKeys.IsHot(key, h.hotThresh) {
		if data, marshalErr := json.Marshal(dest); marshalErr == nil {
			h.localCache.Set(key, data)
		}
	}
	return false, nil
}

// SetWithFanOut writes to N slot-tagged copies of the key.
// Reads pick a random copy → load spread across N cluster nodes.
//
// Key format: "{product:123}:data:0", "{product:123}:data:1", ...
//
// Tradeoff: N× write amplification + N× memory usage.
// Use only for extremely hot read-heavy keys (e.g., homepage featured product).
func (h *HotKeyShardedClient) SetWithFanOut(ctx context.Context, baseKey string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	for i := 0; i < h.fanOutN; i++ {
		shardKey := fmt.Sprintf("%s:shard:%d", baseKey, i)
		if setErr := h.cluster.cluster.Set(ctx, shardKey, data, ttl).Err(); setErr != nil {
			return setErr
		}
	}
	return nil
}

// GetFromFanOut reads from a random fan-out copy.
func (h *HotKeyShardedClient) GetFromFanOut(ctx context.Context, baseKey string, dest any) error {
	shardIdx := rand.Intn(h.fanOutN)
	shardKey := fmt.Sprintf("%s:shard:%d", baseKey, shardIdx)
	data, err := h.cluster.cluster.Get(ctx, shardKey).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

// InvalidateFanOut deletes all fan-out copies.
func (h *HotKeyShardedClient) InvalidateFanOut(ctx context.Context, baseKey string) error {
	keys := make([]string, h.fanOutN)
	for i := 0; i < h.fanOutN; i++ {
		keys[i] = fmt.Sprintf("%s:shard:%d", baseKey, i)
	}
	// Delete one by one (cross-slot safe)
	for _, k := range keys {
		_ = h.cluster.cluster.Del(ctx, k).Err()
	}
	// Also invalidate local cache
	h.localCache.Delete(baseKey)
	return nil
}
