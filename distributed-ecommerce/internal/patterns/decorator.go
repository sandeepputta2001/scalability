// Decorator Pattern
// ──────────────────
// Wraps an object to add behaviour without modifying the original.
// Decorators implement the same interface as the wrapped object,
// so they are transparent to callers.
//
// Used here to add cross-cutting concerns to repositories:
//   - Logging: log every DB call with duration and error
//   - Metrics: count calls, record latency histograms
//   - Retry: automatically retry transient failures
//   - Bulkhead: limit concurrent calls per operation type
//   - Tracing: add distributed trace spans
//
// Tradeoff vs. embedding logic in the repository:
//
//	✓ Single Responsibility: repository only does DB work
//	✓ Composable: stack decorators in any order
//	✓ Testable: test each decorator independently
//	✗ Indirection: harder to trace execution path
//	✗ Interface explosion: every new method needs a decorator wrapper
//
// Example composition:
//
//	repo := NewProductRepository(mc, log)
//	repo = WithLogging(repo, log)
//	repo = WithBulkhead(repo, bulkhead)
//	repo = WithRetry(repo, retryConfig)
//	// All three decorators are transparent to the handler
package patterns

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/models"
	"github.com/distributed-ecommerce/internal/retry"
)

// ProductReader is the interface that decorators wrap.
// Keeping it narrow (only read operations) follows Interface Segregation.
type ProductReader interface {
	GetByID(ctx context.Context, id string) (*models.Product, error)
	List(ctx context.Context, category string, page, pageSize int) ([]*models.Product, error)
	Search(ctx context.Context, query string) ([]*models.Product, error)
}

// ─── Logging Decorator ────────────────────────────────────────────────────────

// LoggingProductReader wraps a ProductReader and logs every call.
type LoggingProductReader struct {
	inner ProductReader
	log   *zap.Logger
}

func WithLogging(inner ProductReader, log *zap.Logger) ProductReader {
	return &LoggingProductReader{inner: inner, log: log}
}

func (d *LoggingProductReader) GetByID(ctx context.Context, id string) (*models.Product, error) {
	start := time.Now()
	result, err := d.inner.GetByID(ctx, id)
	d.log.Debug("ProductReader.GetByID",
		zap.String("id", id),
		zap.Duration("elapsed", time.Since(start)),
		zap.Bool("found", result != nil),
		zap.Error(err))
	return result, err
}

func (d *LoggingProductReader) List(ctx context.Context, category string, page, pageSize int) ([]*models.Product, error) {
	start := time.Now()
	results, err := d.inner.List(ctx, category, page, pageSize)
	d.log.Debug("ProductReader.List",
		zap.String("category", category),
		zap.Int("page", page),
		zap.Int("count", len(results)),
		zap.Duration("elapsed", time.Since(start)),
		zap.Error(err))
	return results, err
}

func (d *LoggingProductReader) Search(ctx context.Context, query string) ([]*models.Product, error) {
	start := time.Now()
	results, err := d.inner.Search(ctx, query)
	d.log.Debug("ProductReader.Search",
		zap.String("query", query),
		zap.Int("count", len(results)),
		zap.Duration("elapsed", time.Since(start)),
		zap.Error(err))
	return results, err
}

// ─── Bulkhead Decorator ───────────────────────────────────────────────────────

// BulkheadProductReader wraps a ProductReader with a concurrency limit.
type BulkheadProductReader struct {
	inner    ProductReader
	bulkhead *Bulkhead
}

func WithBulkhead(inner ProductReader, b *Bulkhead) ProductReader {
	return &BulkheadProductReader{inner: inner, bulkhead: b}
}

func (d *BulkheadProductReader) GetByID(ctx context.Context, id string) (*models.Product, error) {
	var result *models.Product
	err := d.bulkhead.Do(ctx, func(ctx context.Context) error {
		var e error
		result, e = d.inner.GetByID(ctx, id)
		return e
	})
	return result, err
}

func (d *BulkheadProductReader) List(ctx context.Context, category string, page, pageSize int) ([]*models.Product, error) {
	var results []*models.Product
	err := d.bulkhead.Do(ctx, func(ctx context.Context) error {
		var e error
		results, e = d.inner.List(ctx, category, page, pageSize)
		return e
	})
	return results, err
}

func (d *BulkheadProductReader) Search(ctx context.Context, query string) ([]*models.Product, error) {
	var results []*models.Product
	err := d.bulkhead.Do(ctx, func(ctx context.Context) error {
		var e error
		results, e = d.inner.Search(ctx, query)
		return e
	})
	return results, err
}

// ─── Retry Decorator ──────────────────────────────────────────────────────────

// RetryProductReader wraps a ProductReader with automatic retry.
type RetryProductReader struct {
	inner ProductReader
	cfg   retry.Config
	log   *zap.Logger
}

func WithRetry(inner ProductReader, cfg retry.Config, log *zap.Logger) ProductReader {
	return &RetryProductReader{inner: inner, cfg: cfg, log: log}
}

func (d *RetryProductReader) GetByID(ctx context.Context, id string) (*models.Product, error) {
	var result *models.Product
	err := retry.DoWithLog(ctx, d.cfg, d.log, "ProductReader.GetByID", func(ctx context.Context) error {
		var e error
		result, e = d.inner.GetByID(ctx, id)
		return e
	})
	return result, err
}

func (d *RetryProductReader) List(ctx context.Context, category string, page, pageSize int) ([]*models.Product, error) {
	var results []*models.Product
	err := retry.DoWithLog(ctx, d.cfg, d.log, "ProductReader.List", func(ctx context.Context) error {
		var e error
		results, e = d.inner.List(ctx, category, page, pageSize)
		return e
	})
	return results, err
}

func (d *RetryProductReader) Search(ctx context.Context, query string) ([]*models.Product, error) {
	var results []*models.Product
	err := retry.DoWithLog(ctx, d.cfg, d.log, "ProductReader.Search", func(ctx context.Context) error {
		var e error
		results, e = d.inner.Search(ctx, query)
		return e
	})
	return results, err
}
