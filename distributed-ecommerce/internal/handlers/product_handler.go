package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/models"
	"github.com/distributed-ecommerce/internal/repository"
)

const productFreshTTL = 5 * time.Minute
const productStaleTTL = 30 * time.Minute

type ProductHandler struct {
	productRepo *repository.ProductRepository
	cache       *cache.Client
	log         *zap.Logger
}

func NewProductHandler(repo *repository.ProductRepository, cache *cache.Client, log *zap.Logger) *ProductHandler {
	return &ProductHandler{productRepo: repo, cache: cache, log: log}
}

// CreateProduct godoc
// POST /api/v1/products
// Uses write-through caching: DB write + cache write in one operation.
func (h *ProductHandler) CreateProduct(c *gin.Context) {
	var p models.Product
	if err := c.ShouldBindJSON(&p); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Write-through: write to DB first, then populate cache
	err := h.cache.WriteThrough(
		c.Request.Context(),
		cache.ProductKey(p.ID),
		&p,
		productFreshTTL,
		func(ctx context.Context) error {
			return h.productRepo.Create(ctx, &p)
		},
	)
	if err != nil {
		h.log.Error("create product write-through", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create product"})
		return
	}
	c.JSON(http.StatusCreated, p)
}

// GetProduct godoc
// GET /api/v1/products/:id?consistency=strong|eventual
//
// Demonstrates three caching strategies selectable via query param:
//   - consistency=strong  → always read from DB (no cache), detect inconsistencies
//   - consistency=stale   → stale-while-revalidate (serve stale, refresh async)
//   - default             → singleflight cache-aside with stampede prevention
func (h *ProductHandler) GetProduct(c *gin.Context) {
	id := c.Param("id")
	consistency := c.DefaultQuery("consistency", "eventual")

	switch consistency {
	case "strong":
		h.getProductStrong(c, id)
	case "stale":
		h.getProductStaleWhileRevalidate(c, id)
	default:
		h.getProductEventual(c, id)
	}
}

// getProductEventual: singleflight + probabilistic early expiry
// Prevents thundering herd; serves slightly stale data is acceptable.
func (h *ProductHandler) getProductEventual(c *gin.Context, id string) {
	var product models.Product
	err := h.cache.GetOrCompute(
		c.Request.Context(),
		cache.ProductKey(id),
		&product,
		productFreshTTL,
		func(ctx context.Context) (any, error) {
			return h.productRepo.GetByID(ctx, id)
		},
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
		return
	}
	c.Header("X-Cache-Strategy", "singleflight-cache-aside")
	c.Header("X-Consistency", "eventual")
	c.JSON(http.StatusOK, product)
}

// getProductStaleWhileRevalidate: serve stale data immediately, refresh async.
// Best for high-traffic products where p99 latency matters more than freshness.
func (h *ProductHandler) getProductStaleWhileRevalidate(c *gin.Context, id string) {
	var product models.Product
	isStale, err := h.cache.StaleWhileRevalidate(
		c.Request.Context(),
		cache.ProductKey(id),
		&product,
		productFreshTTL,
		productStaleTTL,
		func(ctx context.Context) (any, error) {
			return h.productRepo.GetByID(ctx, id)
		},
	)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
		return
	}
	c.Header("X-Cache-Strategy", "stale-while-revalidate")
	if isStale {
		c.Header("X-Cache", "STALE")
		c.Header("Warning", "110 - Response is Stale")
	} else {
		c.Header("X-Cache", "FRESH")
	}
	c.JSON(http.StatusOK, product)
}

// getProductStrong: always reads from DB, then compares with cache to detect
// inconsistencies. This is the "read-your-writes" consistency mode.
//
// Tradeoff: highest consistency, highest latency (always hits DB).
// Use for: checkout flow, inventory checks, payment confirmation pages.
func (h *ProductHandler) getProductStrong(c *gin.Context, id string) {
	// Always read from DB (source of truth)
	product, err := h.productRepo.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
		return
	}

	// Check if cache has a different value — detect inconsistency window
	var cached models.Product
	if cacheErr := h.cache.GetProduct(c.Request.Context(), id, &cached); cacheErr == nil {
		if cached.Stock != product.Stock || cached.Price != product.Price {
			// Cache/DB inconsistency detected — log it
			h.cache.RecordInconsistency(c.Request.Context(), cache.InconsistencyEvent{
				Key:        cache.ProductKey(id),
				CacheVal:   strconv.Itoa(cached.Stock),
				DBVal:      strconv.Itoa(product.Stock),
				DetectedAt: time.Now(),
			})
			// Heal the cache immediately
			_ = h.cache.CacheProduct(c.Request.Context(), id, product)
		}
	} else if !errors.Is(cacheErr, redis.Nil) {
		h.log.Warn("cache read error during strong consistency check", zap.Error(cacheErr))
	}

	c.Header("X-Cache-Strategy", "strong-consistency")
	c.Header("X-Consistency", "strong")
	c.JSON(http.StatusOK, product)
}

// ListProducts godoc
// GET /api/v1/products?category=electronics&page=1&page_size=20
func (h *ProductHandler) ListProducts(c *gin.Context) {
	category := c.Query("category")
	page, _ := strconv.Atoi(c.DefaultQuery("page", "1"))
	pageSize, _ := strconv.Atoi(c.DefaultQuery("page_size", "20"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 100 {
		pageSize = 20
	}

	products, err := h.productRepo.List(c.Request.Context(), category, page, pageSize)
	if err != nil {
		h.log.Error("list products", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list products"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"products": products, "page": page, "page_size": pageSize})
}

// SearchProducts godoc
// GET /api/v1/products/search?q=laptop
func (h *ProductHandler) SearchProducts(c *gin.Context) {
	q := c.Query("q")
	if q == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "query parameter 'q' is required"})
		return
	}
	products, err := h.productRepo.Search(c.Request.Context(), q)
	if err != nil {
		h.log.Error("search products", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "search failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"products": products})
}
