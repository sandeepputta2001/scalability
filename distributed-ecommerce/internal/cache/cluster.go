// cluster.go implements Redis Cluster support with:
//
//	Redis Cluster = Sharding + Replication in one topology
//	─────────────────────────────────────────────────────
//	Sharding:     16384 hash slots distributed across N master nodes.
//	              Key → CRC16(key) % 16384 → slot → master node.
//	              Adding/removing masters triggers slot migration (resharding).
//
//	Replication:  Each master has M replicas. Replicas mirror the master
//	              asynchronously. On master failure, a replica is promoted
//	              automatically (no Sentinel needed — cluster handles it).
//
//	Our topology (3 masters × 1 replica = 6 nodes):
//	  redis-cluster-1 (master, slots 0-5460)    ← redis-cluster-4 (replica)
//	  redis-cluster-2 (master, slots 5461-10922) ← redis-cluster-5 (replica)
//	  redis-cluster-3 (master, slots 10923-16383)← redis-cluster-6 (replica)
//
//	Key routing:
//	  Writes → master that owns the slot
//	  Reads  → replica of that master (ReadFromReplica=true)
//	  MOVED  → client follows redirect to correct master (transparent)
//	  ASK    → client follows redirect during slot migration (transparent)
//
//	Hash Tags:
//	  Keys with {tag} use only the tag for slot calculation.
//	  {user:123}:cart and {user:123}:session → same slot → same node.
//	  This allows multi-key operations (MGET, pipelines, Lua scripts)
//	  on related keys without cross-slot errors.
//
//	Tradeoffs vs. Standalone + Sentinel:
//	  ✓ Horizontal write scaling (each master handles a fraction of keys)
//	  ✓ No single point of failure (automatic failover, no Sentinel)
//	  ✓ Linear memory scaling (add masters to increase capacity)
//	  ✗ Multi-key ops (MGET, pipelines) require same hash slot
//	  ✗ Lua scripts must only touch keys on the same slot
//	  ✗ No SELECT (database 0 only)
//	  ✗ Resharding causes brief latency spikes during slot migration
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/config"
)

// ClusterClient wraps redis.ClusterClient with topology-aware routing,
// hot-key detection, and cross-slot operation helpers.
type ClusterClient struct {
	cluster *redis.ClusterClient
	ttl     config.RedisTTLConfig
	hotKeys *HotKeyDetector
	log     *zap.Logger
}

// NewClusterClient connects to a Redis Cluster.
// ReadFromReplica=true routes read commands to replica nodes, reducing
// master load at the cost of potentially stale reads (replication lag).
func NewClusterClient(cfg config.RedisConfig, log *zap.Logger) (*ClusterClient, error) {
	cc := cfg.Cluster
	maxRedirects := cc.MaxRedirects
	if maxRedirects == 0 {
		maxRedirects = 8 // follow up to 8 MOVED/ASK redirects
	}

	clusterOpts := &redis.ClusterOptions{
		Addrs:    cc.Addrs,
		Password: cc.Password,

		// MaxRedirects: how many MOVED/ASK redirects to follow.
		// MOVED = key belongs to a different master (permanent).
		// ASK   = key is being migrated (temporary, during resharding).
		MaxRedirects: maxRedirects,

		// ReadOnly=true + RouteRandomly=true: reads go to a random replica.
		// Tradeoff: lower master CPU + higher read throughput vs. stale reads.
		ReadOnly:      cc.ReadFromReplica,
		RouteRandomly: cc.ReadFromReplica,

		// RouteByLatency: among replicas, pick the one with lowest latency.
		// Tradeoff: better p99 latency vs. slightly uneven load distribution.
		RouteByLatency: false,

		PoolSize:     20,
		MinIdleConns: 5,

		// DialTimeout / ReadTimeout / WriteTimeout
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}

	client := redis.NewClusterClient(clusterOpts)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis cluster ping: %w", err)
	}

	// Log slot distribution for observability
	logClusterInfo(ctx, client, log)

	return &ClusterClient{
		cluster: client,
		ttl:     cfg.TTL,
		hotKeys: NewHotKeyDetector(100, 1000), // top-100 keys, window=1000 ops
		log:     log,
	}, nil
}

// ─── Core operations ──────────────────────────────────────────────────────────

func (c *ClusterClient) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.cluster.Set(ctx, key, data, ttl).Err()
}

func (c *ClusterClient) Get(ctx context.Context, key string, dest any) error {
	c.hotKeys.Record(key)
	data, err := c.cluster.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

func (c *ClusterClient) Delete(ctx context.Context, keys ...string) error {
	// In cluster mode, DEL with multiple keys only works if all keys are on
	// the same slot. We delete one by one to avoid CROSSSLOT errors.
	// Tradeoff: N round-trips vs. 1 for standalone. Use hash tags to avoid this.
	pipe := c.cluster.Pipeline()
	for _, k := range keys {
		pipe.Del(ctx, k)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// ─── Hash-tag aware multi-key operations ─────────────────────────────────────

// MGetSameSlot fetches multiple keys that MUST share the same hash slot.
// Use hash tags: {user:123}:cart, {user:123}:session → same slot.
//
// If keys span multiple slots, this returns a CROSSSLOT error.
// Tradeoff: forces key design discipline; enables O(1) multi-key reads.
func (c *ClusterClient) MGetSameSlot(ctx context.Context, keys ...string) ([]any, error) {
	return c.cluster.MGet(ctx, keys...).Result()
}

// PipelineSameSlot executes a pipeline where all keys share the same hash slot.
// Returns the pipeline for the caller to add commands.
func (c *ClusterClient) PipelineSameSlot(ctx context.Context, fn func(pipe redis.Pipeliner) error) error {
	_, err := c.cluster.Pipelined(ctx, fn)
	return err
}

// ─── Cluster-safe distributed lock ───────────────────────────────────────────

// AcquireLock acquires a distributed lock using SET NX on the cluster.
// The lock key must be on a single slot — use a hash tag if needed:
//
//	AcquireLock(ctx, "{order}:lock:123", ...)
//
// Tradeoff vs. Redlock (multi-master lock):
//
//	Single-node lock: simpler, faster, but not safe during master failover.
//	If the master holding the lock fails before replication, the new master
//	won't know about the lock → two clients may hold it simultaneously.
//	For strong lock guarantees, use Redlock across all masters.
func (c *ClusterClient) AcquireLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	return c.cluster.SetNX(ctx, "lock:"+key, value, ttl).Result()
}

func (c *ClusterClient) ReleaseLock(ctx context.Context, key, value string) error {
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	return script.Run(ctx, c.cluster, []string{"lock:" + key}, value).Err()
}

// ─── Redlock across all cluster masters ──────────────────────────────────────

// RedlockAcquire implements a simplified Redlock across all cluster masters.
// Acquires the lock on a majority of masters (N/2 + 1).
//
// This is safe during master failover: even if one master fails after
// granting the lock, the majority still holds it.
//
// Tradeoff: N round-trips (one per master) vs. 1 for single-node lock.
// Use for: order creation, payment processing (high-value operations).
func (c *ClusterClient) RedlockAcquire(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	masters, err := c.getMasters(ctx)
	if err != nil {
		// Fall back to single-node lock
		return c.AcquireLock(ctx, key, value, ttl)
	}

	majority := len(masters)/2 + 1
	acquired := 0
	start := time.Now()

	for _, master := range masters {
		ok, err := master.SetNX(ctx, "lock:"+key, value, ttl).Result()
		if err == nil && ok {
			acquired++
		}
	}

	elapsed := time.Since(start)
	// Lock is valid only if acquired on majority AND within validity time
	if acquired >= majority && elapsed < ttl/2 {
		return true, nil
	}

	// Failed to acquire majority — release all acquired locks
	c.RedlockRelease(ctx, key, value)
	return false, nil
}

// RedlockRelease releases the lock on all masters.
func (c *ClusterClient) RedlockRelease(ctx context.Context, key, value string) {
	masters, err := c.getMasters(ctx)
	if err != nil {
		_ = c.ReleaseLock(ctx, key, value)
		return
	}
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	for _, master := range masters {
		_ = script.Run(ctx, master, []string{"lock:" + key}, value).Err()
	}
}

// ─── Rate limiting (cluster-safe) ────────────────────────────────────────────

// CheckRateLimit uses a hash-tagged key so the INCR + EXPIRE pipeline
// lands on a single slot (no CROSSSLOT error).
// Key format: "rl:{ip}:minute" — hash tag {ip} pins to one slot.
func (c *ClusterClient) CheckRateLimit(ctx context.Context, ip string, limit int) (bool, int64, error) {
	// Hash tag ensures INCR and EXPIRE land on the same slot
	key := fmt.Sprintf("rl:{%s}:%d", ip, time.Now().Unix()/60)
	pipe := c.cluster.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 60*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return true, 0, err
	}
	count := incr.Val()
	return count <= int64(limit), count, nil
}

// ─── Product / Cart / Session helpers ────────────────────────────────────────

// Hash-tagged keys co-locate related data on the same slot.
// {product:ID} ensures product cache and product lock are on the same node.
func ClusterProductKey(id string) string    { return fmt.Sprintf("{product:%s}:data", id) }
func ClusterCartKey(userID string) string   { return fmt.Sprintf("{user:%s}:cart", userID) }
func ClusterSessionKey(token string) string { return fmt.Sprintf("{session:%s}:data", token) }

func (c *ClusterClient) CacheProduct(ctx context.Context, id string, product any) error {
	return c.Set(ctx, ClusterProductKey(id), product, c.ttl.ProductTTL())
}

func (c *ClusterClient) GetProduct(ctx context.Context, id string, dest any) error {
	return c.Get(ctx, ClusterProductKey(id), dest)
}

func (c *ClusterClient) InvalidateProduct(ctx context.Context, id string) error {
	return c.Delete(ctx, ClusterProductKey(id))
}

func (c *ClusterClient) SetCart(ctx context.Context, userID string, cart any) error {
	return c.Set(ctx, ClusterCartKey(userID), cart, c.ttl.CartTTL())
}

func (c *ClusterClient) GetCart(ctx context.Context, userID string, dest any) error {
	return c.Get(ctx, ClusterCartKey(userID), dest)
}

func (c *ClusterClient) DeleteCart(ctx context.Context, userID string) error {
	return c.Delete(ctx, ClusterCartKey(userID))
}

func (c *ClusterClient) SetSession(ctx context.Context, token, userID string) error {
	return c.Set(ctx, ClusterSessionKey(token), userID, c.ttl.UserSessionTTL())
}

func (c *ClusterClient) GetSession(ctx context.Context, token string) (string, error) {
	var s string
	err := c.Get(ctx, ClusterSessionKey(token), &s)
	return s, err
}

// ─── Slot info / observability ────────────────────────────────────────────────

// SlotInfo returns which slot a given key maps to.
func (c *ClusterClient) SlotInfo(ctx context.Context, key string) (string, error) {
	// CRC16 slot calculation (same algorithm Redis uses)
	slot := crc16(key) % 16384
	return fmt.Sprintf("key=%s slot=%d", key, slot), nil
}

// ClusterStats returns per-node key counts and memory usage.
func (c *ClusterClient) ClusterStats(ctx context.Context) ([]NodeStats, error) {
	var stats []NodeStats
	err := c.cluster.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		info, err := shard.Info(ctx, "memory", "keyspace", "replication").Result()
		if err != nil {
			return nil // skip unavailable nodes
		}
		stats = append(stats, NodeStats{
			Addr: shard.Options().Addr,
			Info: info,
		})
		return nil
	})
	return stats, err
}

// NodeStats holds per-node observability data.
type NodeStats struct {
	Addr string `json:"addr"`
	Info string `json:"info"`
}

// HotKeys returns the top-N most accessed keys in the current window.
func (c *ClusterClient) HotKeys() []HotKeyEntry {
	return c.hotKeys.TopN(10)
}

// Close shuts down the cluster client.
func (c *ClusterClient) Close() error {
	return c.cluster.Close()
}

// ClusterMode returns true — this is a cluster client.
func (c *ClusterClient) ClusterMode() bool { return true }

// Master returns the underlying ClusterClient for Kafka consumer idempotency.
func (c *ClusterClient) Master() *redis.ClusterClient {
	return c.cluster
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

func (c *ClusterClient) getMasters(ctx context.Context) ([]*redis.Client, error) {
	var masters []*redis.Client
	err := c.cluster.ForEachMaster(ctx, func(ctx context.Context, master *redis.Client) error {
		masters = append(masters, master)
		return nil
	})
	return masters, err
}

func logClusterInfo(ctx context.Context, client *redis.ClusterClient, log *zap.Logger) {
	nodes, err := client.ClusterNodes(ctx).Result()
	if err != nil {
		log.Warn("could not fetch cluster nodes", zap.Error(err))
		return
	}
	if len(nodes) > 200 {
		nodes = nodes[:200]
	}
	log.Info("redis cluster topology", zap.String("nodes", nodes))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// crc16 computes the CRC16/CCITT-FALSE checksum used by Redis for slot assignment.
func crc16(key string) uint16 {
	// Extract hash tag if present: {tag} → use only tag for slot
	s := key
	if start := indexOf(key, '{'); start >= 0 {
		if end := indexOfFrom(key, '}', start+1); end > start+1 {
			s = key[start+1 : end]
		}
	}
	var crc uint16 = 0
	for i := 0; i < len(s); i++ {
		crc = (crc << 8) ^ crc16tab[((crc>>8)^uint16(s[i]))&0x00FF]
	}
	return crc
}

func indexOf(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func indexOfFrom(s string, b byte, from int) int {
	for i := from; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

var crc16tab = [256]uint16{
	0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
	0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
	0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
	0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
	0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
	0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
	0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
	0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
	0x4864, 0x5845, 0x6826, 0x7807, 0x08e0, 0x18c1, 0x28a2, 0x3883,
	0xc96c, 0xd94d, 0xe92e, 0xf90f, 0x89e8, 0x99c9, 0xa9aa, 0xb98b,
	0x5a55, 0x4a74, 0x7a17, 0x6a36, 0x1ad1, 0x0af0, 0x3a93, 0x2ab2,
	0xdb5d, 0xcb7c, 0xfb1f, 0xeb3e, 0x9bd9, 0x8bf8, 0xbb9b, 0xabba,
	0x6c66, 0x7c47, 0x4c24, 0x5c05, 0x2ce2, 0x3cc3, 0x0ca0, 0x1c81,
	0xed6e, 0xfd4f, 0xcd2c, 0xdd0d, 0xadea, 0xbdcb, 0x8da8, 0x9d89,
	0x7e57, 0x6e76, 0x5e15, 0x4e34, 0x3ed3, 0x2ef2, 0x1e91, 0x0eb0,
	0xff5f, 0xef7e, 0xdf1d, 0xcf3c, 0xbffb, 0xafda, 0x9fb9, 0x8f98,
	0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
	0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
	0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
	0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
	0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
	0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
	0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
	0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
	0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
	0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
	0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
	0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
	0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
	0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
	0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
	0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
}
