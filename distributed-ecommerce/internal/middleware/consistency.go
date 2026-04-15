// consistency.go injects the requested consistency level from HTTP headers/params
// into the request context so all downstream DB and cache calls can honour it.
//
// Clients specify consistency via:
//
//	Header:      X-Consistency: strong|session|bounded-staleness|eventual
//	Query param: ?consistency=strong  (for GET requests)
package middleware

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/distributed-ecommerce/internal/consistency"
)

// ConsistencyMiddleware reads the requested consistency level and injects it
// into the request context. All downstream handlers and repositories read
// this via consistency.FromContext(ctx).
func ConsistencyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		levelStr := c.GetHeader("X-Consistency")
		if levelStr == "" {
			levelStr = c.Query("consistency")
		}

		level, err := consistency.ParseLevel(levelStr)
		if err != nil {
			level = consistency.Eventual
		}

		req := consistency.Request{
			Level:        level,
			SessionToken: c.GetHeader("X-Session-Token"),
		}

		if level == consistency.BoundedStaleness {
			if ms := c.GetHeader("X-Max-Staleness-Ms"); ms != "" {
				var msInt int64
				if _, scanErr := fmt.Sscanf(ms, "%d", &msInt); scanErr == nil {
					req.MaxStaleness = time.Duration(msInt) * time.Millisecond
				}
			}
			if req.MaxStaleness == 0 {
				req.MaxStaleness = 30 * time.Second
			}
		}

		if level == consistency.Session && req.SessionToken != "" {
			req.WriteTimestamp = consistency.LastWrite(req.SessionToken)
		}

		ctx := consistency.WithConsistency(c.Request.Context(), req)
		c.Request = c.Request.WithContext(ctx)

		c.Next()

		// Set response headers so clients know what consistency level was applied
		for k, v := range consistency.ResponseHeaders(level, 0, false) {
			c.Header(k, v)
		}
	}
}
