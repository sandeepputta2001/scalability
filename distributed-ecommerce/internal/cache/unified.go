// unified.go provides a single CacheClient interface that works in both
// standalone (master/replica/sentinel) and cluster modes.
package cache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/config"
)

// CacheClient is the unified interface used by all application code.
type CacheClient interface {
	Set(ctx context.Context, key string, value any, ttl time.Duration) error
	Get(ctx context.Context, key string, dest any) error
	Delete(ctx context.Context, keys ...string) error

	CacheProduct(ctx context.Context, id string, product any) error
	GetProduct(ctx context.Context, id string, dest any) error
	InvalidateProduct(ctx context.Context, id string) error

	SetCart(ctx context.Context, userID string, cart any) error
	GetCart(ctx context.Context, userID string, dest any) error
	DeleteCart(ctx context.Context, userID string) error

	SetSession(ctx context.Context, token, userID string) error
	GetSession(ctx context.Context, token string) (string, error)

	CheckRateLimit(ctx context.Context, ip string, limit int) (bool, int64, error)
	AcquireLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	ReleaseLock(ctx context.Context, key, value string) error

	WriteThrough(ctx context.Context, key string, value any, ttl time.Duration, dbWriter func(ctx context.Context) error) error
	GetOrCompute(ctx context.Context, key string, dest any, ttl time.Duration, compute func(ctx context.Context) (any, error)) error
	StaleWhileRevalidate(ctx context.Context, key string, dest any, freshTTL, staleTTL time.Duration, revalidate func(ctx context.Context) (any, error)) (bool, error)
	TagVersion(ctx context.Context, tag string) (int64, error)
	BumpTagVersion(ctx context.Context, tag string) (int64, error)
	RecordInconsistency(ctx context.Context, event InconsistencyEvent)
	GetInconsistencyLog(ctx context.Context) ([]InconsistencyEvent, error)

	ClusterMode() bool
	Close() error
}

// ─── Standalone Client: implement remaining interface methods ─────────────────

// ClusterMode returns false for standalone client.
func (c *Client) ClusterMode() bool { return false }

// ─── ClusterClient: implement full CacheClient interface ─────────────────────
// ClusterMode() is declared in cluster.go

func (c *ClusterClient) WriteThrough(ctx context.Context, key string, value any, ttl time.Duration, dbWriter func(ctx context.Context) error) error {
	if err := dbWriter(ctx); err != nil {
		return err
	}
	if err := c.Set(ctx, key, value, ttl); err != nil {
		c.log.Warn("cluster write-through cache set failed", zap.String("key", key), zap.Error(err))
	}
	return nil
}

func (c *ClusterClient) GetOrCompute(ctx context.Context, key string, dest any, ttl time.Duration, compute func(ctx context.Context) (any, error)) error {
	if err := c.Get(ctx, key, dest); err == nil {
		return nil
	}
	val, err := compute(ctx)
	if err != nil {
		return err
	}
	_ = c.Set(ctx, key, val, ttl)
	return c.Get(ctx, key, dest)
}

func (c *ClusterClient) StaleWhileRevalidate(ctx context.Context, key string, dest any, freshTTL, staleTTL time.Duration, revalidate func(ctx context.Context) (any, error)) (bool, error) {
	freshKey := "fresh:" + key
	staleKey := "stale:" + key
	if err := c.Get(ctx, freshKey, dest); err == nil {
		return false, nil
	}
	if err := c.Get(ctx, staleKey, dest); err == nil {
		go func() {
			bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			val, err := revalidate(bgCtx)
			if err != nil {
				return
			}
			_ = c.Set(bgCtx, freshKey, val, freshTTL)
			_ = c.Set(bgCtx, staleKey, val, staleTTL)
		}()
		return true, nil
	}
	val, err := revalidate(ctx)
	if err != nil {
		return false, err
	}
	_ = c.Set(ctx, freshKey, val, freshTTL)
	_ = c.Set(ctx, staleKey, val, staleTTL)
	return false, c.Get(ctx, freshKey, dest)
}

func (c *ClusterClient) TagVersion(ctx context.Context, tag string) (int64, error) {
	v, err := c.cluster.Get(ctx, "tag:"+tag).Int64()
	if err == redis.Nil {
		return 1, nil
	}
	return v, err
}

func (c *ClusterClient) BumpTagVersion(ctx context.Context, tag string) (int64, error) {
	return c.cluster.Incr(ctx, "tag:"+tag).Result()
}

func (c *ClusterClient) RecordInconsistency(ctx context.Context, event InconsistencyEvent) {
	data, _ := json.Marshal(event)
	pipe := c.cluster.Pipeline()
	pipe.LPush(ctx, "inconsistency:log", data)
	pipe.LTrim(ctx, "inconsistency:log", 0, 99)
	_, _ = pipe.Exec(ctx)
	c.log.Warn("cache/db inconsistency", zap.String("key", event.Key))
}

func (c *ClusterClient) GetInconsistencyLog(ctx context.Context) ([]InconsistencyEvent, error) {
	items, err := c.cluster.LRange(ctx, "inconsistency:log", 0, 99).Result()
	if err != nil {
		return nil, err
	}
	events := make([]InconsistencyEvent, 0, len(items))
	for _, item := range items {
		var e InconsistencyEvent
		if jsonErr := json.Unmarshal([]byte(item), &e); jsonErr == nil {
			events = append(events, e)
		}
	}
	return events, nil
}

// ─── Factory ──────────────────────────────────────────────────────────────────

// NewCacheClient returns a standalone or cluster client based on config.
func NewCacheClient(cfg config.RedisConfig, log *zap.Logger) (CacheClient, error) {
	if cfg.Cluster.Enabled {
		log.Info("redis mode: cluster (sharded + replicated)",
			zap.Strings("addrs", cfg.Cluster.Addrs),
			zap.Bool("read_from_replica", cfg.Cluster.ReadFromReplica))
		return NewClusterClient(cfg, log)
	}
	log.Info("redis mode: standalone (master/replica/sentinel)")
	return NewClient(cfg, log)
}
