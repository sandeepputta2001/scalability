// Worker runs all Kafka consumer groups and background maintenance jobs.
package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/config"
	"github.com/distributed-ecommerce/internal/db"
	kafkapkg "github.com/distributed-ecommerce/internal/kafka"
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

	// Use unified client (standalone or cluster based on config)
	cacheClient, err := cache.NewCacheClient(cfg.Redis, log)
	if err != nil {
		log.Fatal("redis", zap.Error(err))
	}
	defer cacheClient.Close() //nolint:errcheck

	// Kafka consumer idempotency needs a raw *redis.Client.
	// In standalone mode: use the master. In cluster mode: use the cluster client.
	var redisForKafka redis.UniversalClient
	if cfg.Redis.Cluster.Enabled {
		if cc, ok := cacheClient.(*cache.ClusterClient); ok {
			redisForKafka = cc.Master()
		}
	} else {
		if sc, ok := cacheClient.(*cache.Client); ok {
			redisForKafka = sc.Master()
		}
	}
	if redisForKafka == nil {
		// Fallback: create a standalone client for Kafka idempotency
		redisForKafka = redis.NewClient(&redis.Options{
			Addr:     cfg.Redis.Master.Addr,
			Password: cfg.Redis.Master.Password,
		})
	}

	producer := kafkapkg.NewProducer(cfg.Kafka, log)
	defer producer.Close()

	topicCtx, topicCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer topicCancel()
	if err := kafkapkg.EnsureTopics(topicCtx, cfg.Kafka, log); err != nil {
		log.Warn("topic creation warning", zap.Error(err))
	}

	orderRepo := repository.NewOrderRepository(shardMgr, log)
	productRepo := repository.NewProductRepository(mc, log)

	orderHandler := kafkapkg.NewOrderProcessorHandler(orderRepo, producer, log)
	stockHandler := kafkapkg.NewStockProcessorHandler(productRepo, cacheClient, log)
	cacheInvalidator := kafkapkg.NewCacheInvalidatorHandler(cacheClient, log)
	notifHandler := kafkapkg.NewNotificationHandler(log)
	analyticsHandler := kafkapkg.NewAnalyticsHandler(log)
	dlqHandler := kafkapkg.NewDLQReprocessorHandler(producer, log)

	type consumerDef struct {
		cfg     kafkapkg.ConsumerConfig
		handler kafkapkg.MessageHandler
	}

	consumers := []consumerDef{
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderCreated, GroupID: kafkapkg.GroupOrderProcessor, MaxRetries: 3}, orderHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderConfirmed, GroupID: kafkapkg.GroupOrderProcessor, MaxRetries: 3}, orderHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderShipped, GroupID: kafkapkg.GroupOrderProcessor, MaxRetries: 3}, orderHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderCancelled, GroupID: kafkapkg.GroupOrderProcessor, MaxRetries: 3}, orderHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicStockUpdated, GroupID: kafkapkg.GroupStockProcessor, MaxRetries: 3}, stockHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderCreated, GroupID: kafkapkg.GroupCacheInvalidator, MaxRetries: 2}, cacheInvalidator.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicStockUpdated, GroupID: kafkapkg.GroupCacheInvalidator, MaxRetries: 2}, cacheInvalidator.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicUserRegistered, GroupID: kafkapkg.GroupCacheInvalidator, MaxRetries: 2}, cacheInvalidator.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderCreated, GroupID: kafkapkg.GroupNotificationSvc, MaxRetries: 3}, notifHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderConfirmed, GroupID: kafkapkg.GroupNotificationSvc, MaxRetries: 3}, notifHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicUserRegistered, GroupID: kafkapkg.GroupNotificationSvc, MaxRetries: 3}, notifHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicOrderCreated, GroupID: kafkapkg.GroupAnalyticsSvc, MaxRetries: 1}, analyticsHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicStockUpdated, GroupID: kafkapkg.GroupAnalyticsSvc, MaxRetries: 1}, analyticsHandler.Handle},
		{kafkapkg.ConsumerConfig{Topic: kafkapkg.TopicDLQ, GroupID: "dlq-reprocessor", MaxRetries: 1}, dlqHandler.Handle},
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	for _, cd := range consumers {
		c := kafkapkg.NewConsumer(cfg.Kafka, cd.cfg, producer, redisForKafka, cd.handler, log)
		wg.Add(1)
		go func(consumer *kafkapkg.Consumer) {
			defer wg.Done()
			defer consumer.Close() //nolint:errcheck
			consumer.Run(runCtx)
		}(c)
	}

	repMonitor := db.NewReplicationMonitor(shardMgr, log)
	repMonitor.Start()
	defer repMonitor.Stop()

	log.Info("worker started", zap.Int("consumers", len(consumers)))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("worker shutting down...")
	runCancel()
	wg.Wait()
	log.Info("worker stopped")
}
