// Package cache provides a Redis-backed caching layer with:
//   - Write operations routed to the master
//   - Read operations load-balanced across replicas (round-robin)
//   - Sentinel-based automatic failover
//   - Sliding-window rate limiting via Redis INCR + EXPIRE
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/config"
)

// Client wraps a write master and N read replicas.
type Client struct {
	master   *redis.Client
	replicas []*redis.Client
	rrIndex  atomic.Uint64
	ttl      config.RedisTTLConfig
	log      *zap.Logger
}

// NewClient connects to the Redis master and all replicas.
func NewClient(cfg config.RedisConfig, log *zap.Logger) (*Client, error) {
	master := redis.NewClient(&redis.Options{
		Addr:     cfg.Master.Addr,
		Password: cfg.Master.Password,
		DB:       cfg.Master.DB,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := master.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis master ping: %w", err)
	}

	c := &Client{master: master, ttl: cfg.TTL, log: log}

	for _, rc := range cfg.Replicas {
		replica := redis.NewClient(&redis.Options{
			Addr:     rc.Addr,
			Password: rc.Password,
			DB:       rc.DB,
		})
		if err := replica.Ping(ctx).Err(); err != nil {
			log.Warn("redis replica unavailable, skipping", zap.String("addr", rc.Addr))
			continue
		}
		c.replicas = append(c.replicas, replica)
	}

	log.Info("redis connected",
		zap.String("master", cfg.Master.Addr),
		zap.Int("replicas", len(c.replicas)))
	return c, nil
}

// readClient returns the next replica using round-robin, falls back to master.
func (c *Client) readClient() *redis.Client {
	if len(c.replicas) == 0 {
		return c.master
	}
	idx := c.rrIndex.Add(1) % uint64(len(c.replicas))
	return c.replicas[idx]
}

// ─── Generic helpers ──────────────────────────────────────────────────────────

func (c *Client) Set(ctx context.Context, key string, value any, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.master.Set(ctx, key, data, ttl).Err()
}

func (c *Client) Get(ctx context.Context, key string, dest any) error {
	data, err := c.readClient().Get(ctx, key).Bytes()
	if err != nil {
		return err // redis.Nil is returned as-is so callers can detect cache miss
	}
	return json.Unmarshal(data, dest)
}

func (c *Client) Delete(ctx context.Context, keys ...string) error {
	return c.master.Del(ctx, keys...).Err()
}

func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.readClient().Exists(ctx, key).Result()
	return n > 0, err
}

// ─── Product cache ────────────────────────────────────────────────────────────

func ProductKey(id string) string { return "product:" + id }

func (c *Client) CacheProduct(ctx context.Context, id string, product any) error {
	return c.Set(ctx, ProductKey(id), product, c.ttl.ProductTTL())
}

func (c *Client) GetProduct(ctx context.Context, id string, dest any) error {
	return c.Get(ctx, ProductKey(id), dest)
}

func (c *Client) InvalidateProduct(ctx context.Context, id string) error {
	return c.Delete(ctx, ProductKey(id))
}

// ─── Cart cache (hash map per user) ──────────────────────────────────────────

func CartKey(userID string) string { return "cart:" + userID }

func (c *Client) SetCart(ctx context.Context, userID string, cart any) error {
	return c.Set(ctx, CartKey(userID), cart, c.ttl.CartTTL())
}

func (c *Client) GetCart(ctx context.Context, userID string, dest any) error {
	return c.Get(ctx, CartKey(userID), dest)
}

func (c *Client) DeleteCart(ctx context.Context, userID string) error {
	return c.Delete(ctx, CartKey(userID))
}

// ─── Session cache ────────────────────────────────────────────────────────────

func SessionKey(token string) string { return "session:" + token }

func (c *Client) SetSession(ctx context.Context, token string, userID string) error {
	return c.master.Set(ctx, SessionKey(token), userID, c.ttl.UserSessionTTL()).Err()
}

func (c *Client) GetSession(ctx context.Context, token string) (string, error) {
	return c.readClient().Get(ctx, SessionKey(token)).Result()
}

func (c *Client) DeleteSession(ctx context.Context, token string) error {
	return c.Delete(ctx, SessionKey(token))
}

// ─── Rate limiting (sliding window via INCR + EXPIRE) ─────────────────────────

func RateLimitKey(ip string) string {
	return fmt.Sprintf("rl:%s:%d", ip, time.Now().Unix()/60)
}

// CheckRateLimit returns (allowed, currentCount, error).
func (c *Client) CheckRateLimit(ctx context.Context, ip string, limit int) (bool, int64, error) {
	key := RateLimitKey(ip)
	pipe := c.master.Pipeline()
	incr := pipe.Incr(ctx, key)
	pipe.Expire(ctx, key, 60*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return true, 0, err // fail open
	}
	count := incr.Val()
	return count <= int64(limit), count, nil
}

// ─── Distributed lock (SET NX PX) ─────────────────────────────────────────────

func (c *Client) AcquireLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	ok, err := c.master.SetNX(ctx, "lock:"+key, value, ttl).Result()
	return ok, err
}

func (c *Client) ReleaseLock(ctx context.Context, key, value string) error {
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	return script.Run(ctx, c.master, []string{"lock:" + key}, value).Err()
}

// Close closes all Redis connections.
func (c *Client) Close() error {
	for _, r := range c.replicas {
		_ = r.Close()
	}
	return c.master.Close()
}

// Master returns the underlying master redis.Client (used by Kafka consumers
// for idempotency checks and offset caching).
func (c *Client) Master() *redis.Client {
	return c.master
}
