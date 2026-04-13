// Worker processes background jobs: order status updates, cache warming,
// and replication lag monitoring.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/config"
	"github.com/distributed-ecommerce/internal/db"
	"github.com/distributed-ecommerce/internal/models"
	mongoClient "github.com/distributed-ecommerce/internal/mongo"
	"github.com/distributed-ecommerce/internal/repository"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	cfgPath := os.Getenv("CONFIG_PATH")
	if cfgPath == "" {
		cfgPath = "configs/app.yaml"
	}
	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatal("load config", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	shardMgr, err := db.NewShardManager(ctx, cfg.Postgres, log)
	if err != nil {
		log.Fatal("shard manager", zap.Error(err))
	}
	defer shardMgr.Close()

	mc, err := mongoClient.NewClient(ctx, cfg.MongoDB, log)
	if err != nil {
		log.Fatal("mongodb", zap.Error(err))
	}
	defer mc.Disconnect(context.Background()) //nolint:errcheck

	redisClient, err := cache.NewClient(cfg.Redis, log)
	if err != nil {
		log.Fatal("redis", zap.Error(err))
	}
	defer redisClient.Close() //nolint:errcheck

	orderRepo := repository.NewOrderRepository(shardMgr, log)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	// Ticker: every 30s auto-confirm pending orders older than 2 minutes
	confirmTicker := time.NewTicker(30 * time.Second)
	// Ticker: every 60s log replication lag metrics
	lagTicker := time.NewTicker(60 * time.Second)

	log.Info("worker started")
	for {
		select {
		case <-confirmTicker.C:
			runOrderConfirmation(context.Background(), orderRepo, log)
		case <-lagTicker.C:
			checkReplicationLag(context.Background(), shardMgr, log)
		case <-quit:
			log.Info("worker stopping")
			confirmTicker.Stop()
			lagTicker.Stop()
			return
		}
	}
}

// runOrderConfirmation scatter-gathers pending orders and confirms them.
func runOrderConfirmation(ctx context.Context, repo *repository.OrderRepository, log *zap.Logger) {
	orders, err := repo.ScatterGatherOrders(ctx, models.OrderStatusPending, 100)
	if err != nil {
		log.Error("scatter gather orders", zap.Error(err))
		return
	}
	confirmed := 0
	for _, o := range orders {
		if time.Since(o.CreatedAt) > 2*time.Minute {
			if err := repo.UpdateStatus(ctx, o.ID, o.UserID, models.OrderStatusConfirmed); err != nil {
				log.Warn("confirm order", zap.String("id", o.ID.String()), zap.Error(err))
				continue
			}
			confirmed++
		}
	}
	if confirmed > 0 {
		log.Info("orders confirmed", zap.Int("count", confirmed))
	}
}

// checkReplicationLag queries each shard's replica for pg_last_xact_replay_timestamp.
func checkReplicationLag(ctx context.Context, sm *db.ShardManager, log *zap.Logger) {
	for _, shard := range sm.AllShards() {
		for i, replica := range shard.Replicas {
			var lagSeconds float64
			err := replica.QueryRow(ctx, `
				SELECT EXTRACT(EPOCH FROM (NOW() - pg_last_xact_replay_timestamp()))
			`).Scan(&lagSeconds)
			if err != nil {
				log.Warn("replication lag check failed",
					zap.Int("shard", shard.ID), zap.Int("replica", i), zap.Error(err))
				continue
			}
			log.Info("replication lag",
				zap.Int("shard", shard.ID),
				zap.Int("replica", i),
				zap.Float64("lag_seconds", lagSeconds))
		}
	}
}
