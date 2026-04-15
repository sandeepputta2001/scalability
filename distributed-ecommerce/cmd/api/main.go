package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/config"
	"github.com/distributed-ecommerce/internal/db"
	"github.com/distributed-ecommerce/internal/handlers"
	kafkapkg "github.com/distributed-ecommerce/internal/kafka"
	"github.com/distributed-ecommerce/internal/loadbalancer"
	"github.com/distributed-ecommerce/internal/middleware"
	mongoClient "github.com/distributed-ecommerce/internal/mongo"
	"github.com/distributed-ecommerce/internal/partition"
	"github.com/distributed-ecommerce/internal/patterns"
	"github.com/distributed-ecommerce/internal/repository"
	"github.com/distributed-ecommerce/internal/retry"
)

func main() {
	log, _ := zap.NewProduction()
	defer log.Sync()

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

	if err := db.RunMigrations(context.Background(), shardMgr, log); err != nil {
		log.Fatal("migrations", zap.Error(err))
	}

	repMonitor := db.NewReplicationMonitor(shardMgr, log)
	repMonitor.Start()
	defer repMonitor.Stop()

	joiner := db.NewCrossShardJoiner(shardMgr, log)

	replicaNodes := buildReplicaNodes(shardMgr)
	replicaLBAlgo := loadbalancer.NewWeightedRoundRobin(replicaNodes)
	replicaHealthChecker := loadbalancer.NewHealthChecker(
		replicaNodes,
		func(hctx context.Context, node *loadbalancer.Node) error {
			pool := getPoolByAddr(shardMgr, node.Addr)
			if pool == nil {
				return fmt.Errorf("pool not found")
			}
			return pool.Ping(hctx)
		},
		10*time.Second, log,
	)
	replicaLB := loadbalancer.New(replicaLBAlgo, replicaHealthChecker, log)

	mongoNodes := []*loadbalancer.Node{
		loadbalancer.NewNode("mongo-primary", "mongo-primary:27017", 2),
		loadbalancer.NewNode("mongo-secondary1", "mongo-secondary1:27017", 1),
		loadbalancer.NewNode("mongo-secondary2", "mongo-secondary2:27017", 1),
	}
	mongoLB := loadbalancer.New(loadbalancer.NewLeastConnections(mongoNodes), nil, log)

	orderRouter := partition.NewRouter(partition.NewHashPartitioner(shardMgr.ShardCount()))
	userRouter := partition.NewRouter(partition.NewHashPartitioner(shardMgr.ShardCount()))
	productRouter := partition.NewRouter(partition.NewRangePartitioner([]string{"M", "Z"}))

	mc, err := mongoClient.NewClient(ctx, cfg.MongoDB, log)
	if err != nil {
		log.Fatal("mongodb", zap.Error(err))
	}
	defer mc.Disconnect(context.Background())

	redisClient, err := cache.NewCacheClient(cfg.Redis, log)
	if err != nil {
		log.Fatal("redis", zap.Error(err))
	}
	defer redisClient.Close()

	writeBehind := cache.NewWriteBehindBuffer(redisClient, 1000, log)
	defer writeBehind.Stop()

	producer := kafkapkg.NewProducer(cfg.Kafka, log)
	defer producer.Close()

	topicCtx, topicCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer topicCancel()
	if err := kafkapkg.EnsureTopics(topicCtx, cfg.Kafka, log); err != nil {
		log.Warn("kafka topic setup warning", zap.Error(err))
	}

	userRepo := repository.NewUserRepository(shardMgr, log)
	productRepo := repository.NewProductRepository(mc, log)
	orderRepo := repository.NewOrderRepository(shardMgr, repMonitor, log)

	authHandler := handlers.NewAuthHandler(userRepo, producer, cfg.App.JWTSecret, cfg.App.JWTExpiryHours, log)
	productHandler := handlers.NewProductHandler(productRepo, redisClient, log)
	orderHandler := handlers.NewOrderHandler(orderRepo, productRepo, redisClient, producer, log)
	cartHandler := handlers.NewCartHandler(productRepo, redisClient, log)
	adminHandler := handlers.NewAdminHandler(joiner, repMonitor, shardMgr, redisClient, log)
	lbHandler := handlers.NewLBPartitionHandler(replicaLB, mongoLB, orderRouter, userRouter, productRouter, log)

	// ── Design Patterns wiring ────────────────────────────────────────────────
	// Bulkhead registry: isolate resource pools per operation type
	bulkheadReg := patterns.NewBulkheadRegistry()
	bulkheadReg.Register(patterns.NewBulkhead("product-reads", 200, 100*time.Millisecond, log))
	bulkheadReg.Register(patterns.NewBulkhead("order-writes", 50, 500*time.Millisecond, log))
	bulkheadReg.Register(patterns.NewBulkhead("payment-calls", 20, 2*time.Second, log))
	bulkheadReg.Register(patterns.NewBulkhead("cache-reads", 500, 50*time.Millisecond, log))

	// CQRS buses
	cmdBus := patterns.NewCommandBus()
	queryBus := patterns.NewQueryBus()

	// Saga orchestrator
	sagaOrchestrator := patterns.NewOrchestrator(log)

	// In-process event bus (async, for cache invalidation and metrics)
	eventBus := patterns.NewEventBus(true, log)
	eventBus.Subscribe("product.updated", func(ctx context.Context, event patterns.DomainEvent) error {
		if e, ok := event.(patterns.ProductUpdatedEvent); ok {
			return redisClient.InvalidateProduct(ctx, e.ProductID)
		}
		return nil
	})
	eventBus.Subscribe("cache.invalidate", func(ctx context.Context, event patterns.DomainEvent) error {
		if e, ok := event.(patterns.CacheInvalidationEvent); ok {
			return redisClient.Delete(ctx, e.Keys...)
		}
		return nil
	})

	// Circuit breakers (collected for observability)
	kafkaCB := retry.NewCircuitBreaker("kafka-publish", 5, 30*time.Second, log)
	dbCB := retry.NewCircuitBreaker("db-primary", 10, 60*time.Second, log)
	allCBs := []*retry.CircuitBreaker{kafkaCB, dbCB}

	patternsHandler := handlers.NewPatternsHandler(
		bulkheadReg, cmdBus, queryBus, sagaOrchestrator, allCBs, log,
	)

	_ = eventBus // used by subscriptions above

	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.RateLimit(redisClient, cfg.Redis.RateLimit.RequestsPerMinute, log))
	r.Use(middleware.ConsistencyMiddleware())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "shards": shardMgr.ShardCount(), "service": "distributed-ecommerce"})
	})

	v1 := r.Group("/api/v1")
	{
		auth := v1.Group("/auth")
		auth.POST("/register", authHandler.Register)
		auth.POST("/login", authHandler.Login)

		products := v1.Group("/products")
		products.GET("", productHandler.ListProducts)
		products.GET("/search", productHandler.SearchProducts)
		products.GET("/:id", productHandler.GetProduct)
		products.POST("", middleware.JWTAuth(cfg.App.JWTSecret, log), productHandler.CreateProduct)

		protected := v1.Group("")
		protected.Use(middleware.JWTAuth(cfg.App.JWTSecret, log))
		{
			protected.GET("/orders", orderHandler.ListOrders)
			protected.POST("/orders", orderHandler.CreateOrder)
			protected.GET("/orders/:id", orderHandler.GetOrder)
			protected.GET("/cart", cartHandler.GetCart)
			protected.POST("/cart/items", cartHandler.AddItem)
			protected.DELETE("/cart/items/:product_id", cartHandler.RemoveItem)
			protected.DELETE("/cart", cartHandler.ClearCart)
		}

		admin := v1.Group("/admin")
		admin.Use(middleware.JWTAuth(cfg.App.JWTSecret, log))
		{
			admin.GET("/shards", adminHandler.GetShardStats)
			admin.GET("/replication-lag", adminHandler.GetReplicationLag)
			admin.GET("/orders-with-users", adminHandler.CrossShardOrdersWithUsers)
			admin.GET("/cache-inconsistencies", adminHandler.GetCacheInconsistencies)
			admin.GET("/cache-tag/:tag", adminHandler.GetCacheTagVersion)
			admin.POST("/cache-tag/:tag/bump", adminHandler.BumpCacheTag)
			admin.GET("/redis-cluster", adminHandler.GetRedisClusterStats)
			admin.GET("/redis-hot-keys", adminHandler.GetHotKeys)
			admin.GET("/redis-slot", adminHandler.GetSlotInfo)
			admin.GET("/lb-stats", lbHandler.GetLBStats)
			admin.GET("/partition-report", lbHandler.GetPartitionReport)
			admin.GET("/route-key", lbHandler.RouteKey)
			admin.GET("/outbox-stats", adminHandler.GetOutboxStats)
			// Design patterns observability
			admin.GET("/bulkheads", patternsHandler.GetBulkheadStats)
			admin.GET("/circuit-breakers", patternsHandler.GetCircuitBreakerStats)
			admin.POST("/run-saga", patternsHandler.RunOrderSaga)
			admin.GET("/spec-demo", patternsHandler.DemoSpecification)
			admin.POST("/cqrs-demo", patternsHandler.DemoCQRS)
			admin.POST("/builder-demo", patternsHandler.DemoBuilder)
		}
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.App.Port),
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		log.Info("server starting", zap.Int("port", cfg.App.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("server error", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("shutting down...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("shutdown error", zap.Error(err))
	}
	log.Info("server stopped")
}

func buildReplicaNodes(sm *db.ShardManager) []*loadbalancer.Node {
	var nodes []*loadbalancer.Node
	for _, shard := range sm.AllShards() {
		nodes = append(nodes, loadbalancer.NewNode(
			fmt.Sprintf("shard%d-primary", shard.ID),
			fmt.Sprintf("shard-%d-primary", shard.ID),
			1,
		))
	}
	return nodes
}

func getPoolByAddr(sm *db.ShardManager, _ string) *pgxpool.Pool {
	for _, shard := range sm.AllShards() {
		return shard.Primary
	}
	return nil
}
