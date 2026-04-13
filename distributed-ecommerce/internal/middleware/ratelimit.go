package middleware

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/cache"
)

// RateLimit enforces per-IP sliding-window rate limiting via Redis.
func RateLimit(rc *cache.Client, limit int, log *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		ip := c.ClientIP()
		allowed, count, err := rc.CheckRateLimit(c.Request.Context(), ip, limit)
		if err != nil {
			log.Warn("rate limit check failed, allowing request", zap.Error(err))
			c.Next()
			return
		}
		c.Header("X-RateLimit-Limit", "100")
		c.Header("X-RateLimit-Remaining", fmt.Sprintf("%d", max(0, int64(limit)-count)))
		if !allowed {
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "rate limit exceeded"})
			return
		}
		c.Next()
	}
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
