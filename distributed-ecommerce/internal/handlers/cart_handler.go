package handlers

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
	"github.com/distributed-ecommerce/internal/models"
	"github.com/distributed-ecommerce/internal/repository"
)

// CartHandler manages shopping carts stored entirely in Redis.
// Redis is the perfect fit here: carts are ephemeral, frequently updated,
// and don't need durable storage.
type CartHandler struct {
	productRepo *repository.ProductRepository
	cache       *cache.Client
	log         *zap.Logger
}

func NewCartHandler(productRepo *repository.ProductRepository, cache *cache.Client, log *zap.Logger) *CartHandler {
	return &CartHandler{productRepo: productRepo, cache: cache, log: log}
}

// GetCart godoc
// GET /api/v1/cart
func (h *CartHandler) GetCart(c *gin.Context) {
	userID := c.GetString("user_id")
	cart, err := h.getOrCreateCart(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get cart"})
		return
	}
	c.JSON(http.StatusOK, cart)
}

// AddItem godoc
// POST /api/v1/cart/items
func (h *CartHandler) AddItem(c *gin.Context) {
	userID := c.GetString("user_id")

	var req models.AddToCartRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Validate product exists
	product, err := h.productRepo.GetByID(c.Request.Context(), req.ProductID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
		return
	}

	cart, err := h.getOrCreateCart(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get cart"})
		return
	}

	// Update or append item
	found := false
	for i, item := range cart.Items {
		if item.ProductID == req.ProductID {
			cart.Items[i].Quantity += req.Quantity
			found = true
			break
		}
	}
	if !found {
		cart.Items = append(cart.Items, models.CartItem{
			ProductID: req.ProductID,
			Name:      product.Name,
			Quantity:  req.Quantity,
			UnitPrice: product.Price,
		})
	}
	cart.UpdatedAt = time.Now()

	if err := h.cache.SetCart(c.Request.Context(), userID, cart); err != nil {
		h.log.Error("set cart", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update cart"})
		return
	}
	c.JSON(http.StatusOK, cart)
}

// RemoveItem godoc
// DELETE /api/v1/cart/items/:product_id
func (h *CartHandler) RemoveItem(c *gin.Context) {
	userID := c.GetString("user_id")
	productID := c.Param("product_id")

	cart, err := h.getOrCreateCart(c, userID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get cart"})
		return
	}

	filtered := cart.Items[:0]
	for _, item := range cart.Items {
		if item.ProductID != productID {
			filtered = append(filtered, item)
		}
	}
	cart.Items = filtered
	cart.UpdatedAt = time.Now()

	if err := h.cache.SetCart(c.Request.Context(), userID, cart); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update cart"})
		return
	}
	c.JSON(http.StatusOK, cart)
}

// ClearCart godoc
// DELETE /api/v1/cart
func (h *CartHandler) ClearCart(c *gin.Context) {
	userID := c.GetString("user_id")
	if err := h.cache.DeleteCart(c.Request.Context(), userID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to clear cart"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "cart cleared"})
}

func (h *CartHandler) getOrCreateCart(c *gin.Context, userID string) (*models.Cart, error) {
	var cart models.Cart
	err := h.cache.GetCart(c.Request.Context(), userID, &cart)
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if errors.Is(err, redis.Nil) {
		cart = models.Cart{UserID: userID, Items: []models.CartItem{}, UpdatedAt: time.Now()}
	}
	return &cart, nil
}
