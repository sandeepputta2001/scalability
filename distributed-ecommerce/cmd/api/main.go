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
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/config"
	"github.com/distributed-ecommerce/internal/db"
	"github.com/distributed-ecommerce/internal/handlers"
	"github.com/distributed-ecommerce/internal/middleware"
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

	// ── PostgreSQL shards (consistent hash ring) ──────────────────────────────
	shardMgr, err := db.NewShardManager(ctx, cfg.Postgres, log)
	if err != nil {
		log.Fatal("shard manager", zap.Error(err))
	}
	defer shardMgr.Close()

	if err := db.RunMigrations(context.Background(), shardMgr, log); err != nil {
		log.Fatal("migrations", zap.Error(err))
	}

	// ── Replication monitor (lag-aware read routing) ──────────────────────────
	repMonitor := db.NewReplicationMonitor(shardMgr, log)
	repMonitor.Start()
	defer repMonitor.Stop()

	// ── Cross-shard join engine ───────────────────────────────────────────────
	joiner := db.NewCrossShardJoiner(shardMgr, log)

	// ── MongoDB (replica set, secondaryPreferred reads) ───────────────────────
	mc, err := mongoClient.NewClient(ctx, cfg.MongoDB, log)
	if err != nil {
		log.Fatal("mongodb", zap.Error(err))
	}
	defer mc.Disconnect(context.Background()) //nolint:errcheck

	// ── Redis (master/replica + sentinel) ────────────────────────────────────
	redisClient, err := cache.NewClient(cfg.Redis, log)
	if err != nil {
		log.Fatal("redis", zap.Error(err))
	}
	defer redisClient.Close() //nolint:errcheck

	// ── Write-behind buffer (async DB writes) ─────────────────────────────────
	writeBehind := cache.NewWriteBehindBuffer(redisClient, 1000, log)
	defer writeBehind.Stop()

	// ── Repositories ──────────────────────────────────────────────────────────
	userRepo := repository.NewUserRepository(shardMgr, log)
	productRepo := repository.NewProductRepository(mc, log)
	orderRepo := repository.NewOrderRepository(shardMgr, log)

	// ── Handlers ──────────────────────────────────────────────────────────────
	authHandler := handlers.NewAuthHandler(userRepo, cfg.App.JWTSecret, cfg.App.JWTExpiryHours, log)
	productHandler := handlers.NewProductHandler(productRepo, redisClient, log)
	orderHandler := handlers.NewOrderHandler(orderRepo, productRepo, redisClient, log)
	cartHandler := handlers.NewCartHandler(productRepo, redisClient, log)
	adminHandler := handlers.NewAdminHandler(joiner, repMonitor, shardMgr, redisClient, log)

	// ── Router ────────────────────────────────────────────────────────────────
	if cfg.App.Env == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.RateLimit(redisClient, cfg.Redis.RateLimit.RequestsPerMinute, log))

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":            "ok",
			"shards":            shardMgr.ShardCount(),
			"ring_distribution": shardMgr.RingDistribution(),
			"service":           "distributed-ecommerce",
		})
	})

	v1 := r.Group("/api/v1")
	{
		auth := v1.Group("/auth")
		auth.POST("/register", authHandler.Register)
		auth.POST("/login", authHandler.Login)

		// Products: GET supports ?consistency=strong|stale|eventual
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

		// Admin: observability + distributed systems diagnostics
		admin := v1.Group("/admin")
		admin.Use(middleware.JWTAuth(cfg.App.JWTSecret, log))
		{
			// Shard stats + ring distribution
			admin.GET("/shards", adminHandler.GetShardStats)
			// Replication lag per shard/replica
			admin.GET("/replication-lag", adminHandler.GetReplicationLag)
			// Cross-shard scatter-gather + application-level join
			admin.GET("/orders-with-users", adminHandler.CrossShardOrdersWithUsers)
			// Cache inconsistency audit log
			admin.GET("/cache-inconsistencies", adminHandler.GetCacheInconsistencies)
			// Tag-based cache invalidation
			admin.GET("/cache-tag/:tag", adminHandler.GetCacheTagVersion)
			admin.POST("/cache-tag/:tag/bump", adminHandler.BumpCacheTag)
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
