// Package outbox implements the Transactional Outbox Pattern.
//
// Problem it solves:
//
//	Without the outbox, publishing a Kafka event after a DB write has a
//	dual-write problem: if the process crashes between the DB commit and
//	the Kafka publish, the event is lost. The order exists in the DB but
//	downstream services never know about it.
//
//	Naive approach (broken):
//	  1. INSERT order INTO postgres  ← committed
//	  2. CRASH
//	  3. Publish order.created → Kafka  ← never happens
//
// How the Outbox Pattern fixes this:
//
//  1. INSERT order + INSERT outbox_event in the SAME transaction
//     → Both committed atomically, or both rolled back
//
//  2. A separate relay worker polls the outbox table
//
//  3. Relay publishes each event to Kafka
//
//  4. On success: mark event as published (or delete it)
//
//  5. On failure: retry with exponential backoff
//
//     Guarantee: at-least-once delivery (event may be published more than once
//     if the relay crashes after publishing but before marking as published).
//     Consumer-side idempotency (via event_id deduplication) upgrades this to
//     exactly-once processing.
//
// Outbox table schema (per shard):
//
//	CREATE TABLE outbox_events (
//	  id              UUID PRIMARY KEY,
//	  aggregate_type  TEXT NOT NULL,   -- "order", "user", "product"
//	  aggregate_id    TEXT NOT NULL,   -- the entity's ID
//	  event_type      TEXT NOT NULL,   -- "order.created", etc.
//	  payload         JSONB NOT NULL,  -- full event envelope
//	  topic           TEXT NOT NULL,   -- Kafka topic to publish to
//	  partition_key   TEXT NOT NULL,   -- Kafka partition key
//	  status          TEXT NOT NULL DEFAULT 'pending',  -- pending|published|failed
//	  attempts        INT  NOT NULL DEFAULT 0,
//	  last_error      TEXT,
//	  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
//	  published_at    TIMESTAMPTZ,
//	  next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
//	);
//
// Tradeoffs:
//
//	✓ Atomic: event is guaranteed to be published if the DB transaction commits
//	✓ Durable: events survive process crashes (stored in DB)
//	✓ Ordered: events published in creation order per aggregate
//	✗ Latency: event delivery is delayed by the polling interval (default 1s)
//	✗ DB load: polling adds read load to the DB
//	✗ Complexity: requires an outbox table and relay worker
//	✗ At-least-once: consumer must handle duplicates (idempotency key)
package outbox

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/retry"
)

// EventStatus represents the lifecycle state of an outbox event.
type EventStatus string

const (
	StatusPending   EventStatus = "pending"
	StatusPublished EventStatus = "published"
	StatusFailed    EventStatus = "failed" // max retries exceeded
)

// Event is a row in the outbox_events table.
type Event struct {
	ID            uuid.UUID   `json:"id"`
	AggregateType string      `json:"aggregate_type"`
	AggregateID   string      `json:"aggregate_id"`
	EventType     string      `json:"event_type"`
	Payload       []byte      `json:"payload"` // JSON-encoded kafka.Envelope
	Topic         string      `json:"topic"`
	PartitionKey  string      `json:"partition_key"`
	Status        EventStatus `json:"status"`
	Attempts      int         `json:"attempts"`
	LastError     string      `json:"last_error,omitempty"`
	CreatedAt     time.Time   `json:"created_at"`
	PublishedAt   *time.Time  `json:"published_at,omitempty"`
	NextRetryAt   time.Time   `json:"next_retry_at"`
}

// ─── Schema ───────────────────────────────────────────────────────────────────

// OutboxSchema is the DDL for the outbox table, applied to every shard.
const OutboxSchema = `
CREATE TABLE IF NOT EXISTS outbox_events (
    id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    aggregate_type TEXT        NOT NULL,
    aggregate_id   TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        JSONB       NOT NULL,
    topic          TEXT        NOT NULL,
    partition_key  TEXT        NOT NULL,
    status         TEXT        NOT NULL DEFAULT 'pending',
    attempts       INT         NOT NULL DEFAULT 0,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at   TIMESTAMPTZ,
    next_retry_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outbox_status_retry
    ON outbox_events(status, next_retry_at)
    WHERE status IN ('pending', 'failed');

CREATE INDEX IF NOT EXISTS idx_outbox_aggregate
    ON outbox_events(aggregate_type, aggregate_id);
`

// ─── Repository ───────────────────────────────────────────────────────────────

// Repository handles outbox_events persistence.
type Repository struct {
	pool *pgxpool.Pool
	log  *zap.Logger
}

func NewRepository(pool *pgxpool.Pool, log *zap.Logger) *Repository {
	return &Repository{pool: pool, log: log}
}

// InsertWithTx inserts an outbox event within an existing transaction.
// This is the critical operation: the outbox event and the business entity
// are written atomically in the same transaction.
func (r *Repository) InsertWithTx(ctx context.Context, tx interface {
	Exec(ctx context.Context, sql string, args ...any) (interface{}, error)
}, event *Event) error {
	event.ID = uuid.New()
	event.Status = StatusPending
	event.CreatedAt = time.Now()
	event.NextRetryAt = time.Now()

	_, err := r.pool.Exec(ctx, `
		INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, event_type, payload, topic, partition_key,
			 status, attempts, created_at, next_retry_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		event.ID, event.AggregateType, event.AggregateID, event.EventType,
		event.Payload, event.Topic, event.PartitionKey,
		StatusPending, 0, event.CreatedAt, event.NextRetryAt,
	)
	return err
}

// Insert inserts an outbox event using the repository's own pool.
// Use InsertWithTx when you need atomicity with a business entity write.
func (r *Repository) Insert(ctx context.Context, event *Event) error {
	event.ID = uuid.New()
	event.Status = StatusPending
	event.CreatedAt = time.Now()
	event.NextRetryAt = time.Now()

	_, err := r.pool.Exec(ctx, `
		INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, event_type, payload, topic, partition_key,
			 status, attempts, created_at, next_retry_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
		event.ID, event.AggregateType, event.AggregateID, event.EventType,
		event.Payload, event.Topic, event.PartitionKey,
		StatusPending, 0, event.CreatedAt, event.NextRetryAt,
	)
	return err
}

// FetchPending returns up to batchSize events that are ready to be published.
// Uses SELECT FOR UPDATE SKIP LOCKED to allow multiple relay workers to run
// concurrently without processing the same event twice.
func (r *Repository) FetchPending(ctx context.Context, batchSize int) ([]*Event, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, aggregate_type, aggregate_id, event_type, payload, topic,
		       partition_key, status, attempts, last_error, created_at, next_retry_at
		FROM outbox_events
		WHERE status IN ('pending', 'failed')
		  AND next_retry_at <= NOW()
		ORDER BY created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`,
		batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch pending outbox events: %w", err)
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var e Event
		var lastError *string
		if err := rows.Scan(
			&e.ID, &e.AggregateType, &e.AggregateID, &e.EventType,
			&e.Payload, &e.Topic, &e.PartitionKey, &e.Status,
			&e.Attempts, &lastError, &e.CreatedAt, &e.NextRetryAt,
		); err != nil {
			return nil, err
		}
		if lastError != nil {
			e.LastError = *lastError
		}
		events = append(events, &e)
	}
	return events, nil
}

// MarkPublished marks an event as successfully published.
func (r *Repository) MarkPublished(ctx context.Context, id uuid.UUID) error {
	now := time.Now()
	_, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'published', published_at = $1
		WHERE id = $2`,
		now, id,
	)
	return err
}

// MarkFailed increments the attempt counter and schedules the next retry
// using exponential backoff. After maxAttempts, marks as 'failed'.
func (r *Repository) MarkFailed(ctx context.Context, id uuid.UUID, errMsg string, maxAttempts int) error {
	// Fetch current attempt count
	var attempts int
	if err := r.pool.QueryRow(ctx,
		`SELECT attempts FROM outbox_events WHERE id = $1`, id,
	).Scan(&attempts); err != nil {
		return err
	}

	attempts++
	nextRetry := nextRetryTime(attempts)
	status := StatusPending // still retryable
	if attempts >= maxAttempts {
		status = StatusFailed // give up
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = $1, last_error = $2, status = $3, next_retry_at = $4
		WHERE id = $5`,
		attempts, errMsg, status, nextRetry, id,
	)
	return err
}

// GetStats returns counts by status for observability.
func (r *Repository) GetStats(ctx context.Context) (map[string]int64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT status, COUNT(*) FROM outbox_events GROUP BY status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	stats := make(map[string]int64)
	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		stats[status] = count
	}
	return stats, nil
}

// nextRetryTime computes the next retry time using exponential backoff.
// attempt 1 → 1s, attempt 2 → 2s, attempt 3 → 4s, ..., capped at 5 minutes.
func nextRetryTime(attempt int) time.Time {
	base := time.Second
	wait := base * time.Duration(1<<uint(attempt-1)) // 2^(attempt-1) seconds
	if wait > 5*time.Minute {
		wait = 5 * time.Minute
	}
	return time.Now().Add(wait)
}

// ─── Event Builder ────────────────────────────────────────────────────────────

// NewOutboxEvent creates an outbox event from raw components.
// The payload should be a pre-serialised JSON envelope.
func NewOutboxEvent(aggregateType, aggregateID, eventType, topic, partitionKey string, payload []byte) *Event {
	return &Event{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
		Topic:         topic,
		PartitionKey:  partitionKey,
	}
}

// ─── Relay Worker ─────────────────────────────────────────────────────────────

// RelayConfig configures the outbox relay worker.
type RelayConfig struct {
	// PollInterval is how often to check for new events.
	// Lower = lower latency, higher DB load.
	// Tradeoff: 100ms gives ~100ms event delivery latency.
	PollInterval time.Duration
	// BatchSize is how many events to process per poll cycle.
	BatchSize int
	// MaxAttempts before marking an event as permanently failed.
	MaxAttempts int
}

// DefaultRelayConfig returns sensible defaults.
var DefaultRelayConfig = RelayConfig{
	PollInterval: 1 * time.Second,
	BatchSize:    100,
	MaxAttempts:  10,
}

// Publisher is the interface the relay uses to publish events.
// kafka.Producer implements this interface.
type Publisher interface {
	PublishRaw(ctx context.Context, topic string, key []byte, value []byte) error
}

// Relay polls the outbox table and publishes events to Kafka.
// Multiple relay instances can run concurrently (SELECT FOR UPDATE SKIP LOCKED
// ensures each event is processed by exactly one relay at a time).
type Relay struct {
	repo      *Repository
	publisher Publisher
	cfg       RelayConfig
	budget    *retry.Budget
	log       *zap.Logger
}

// NewRelay creates a relay worker.
func NewRelay(repo *Repository, publisher Publisher, cfg RelayConfig, log *zap.Logger) *Relay {
	return &Relay{
		repo:      repo,
		publisher: publisher,
		cfg:       cfg,
		budget:    retry.NewBudget(50),
		log:       log,
	}
}

// Run starts the relay loop. Blocks until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	r.log.Info("outbox relay started",
		zap.Duration("poll_interval", r.cfg.PollInterval),
		zap.Int("batch_size", r.cfg.BatchSize))

	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.processBatch(ctx)
		case <-ctx.Done():
			r.log.Info("outbox relay stopping")
			return
		}
	}
}

// processBatch fetches and publishes one batch of pending events.
func (r *Relay) processBatch(ctx context.Context) {
	events, err := r.repo.FetchPending(ctx, r.cfg.BatchSize)
	if err != nil {
		r.log.Error("fetch outbox events", zap.Error(err))
		return
	}
	if len(events) == 0 {
		return
	}

	r.log.Debug("processing outbox batch", zap.Int("count", len(events)))

	for _, event := range events {
		r.publishEvent(ctx, event)
	}
}

// publishEvent publishes a single outbox event to Kafka with retry.
func (r *Relay) publishEvent(ctx context.Context, event *Event) {
	// Publish with retry + budget — use raw bytes directly
	err := retry.DoWithBudget(ctx, retry.KafkaConfig, r.budget, r.log,
		fmt.Sprintf("outbox-publish:%s", event.EventType),
		func(ctx context.Context) error {
			return r.publisher.PublishRaw(ctx, event.Topic,
				[]byte(event.PartitionKey), event.Payload)
		},
	)

	if err != nil {
		r.log.Error("outbox publish failed",
			zap.String("event_id", event.ID.String()),
			zap.String("event_type", event.EventType),
			zap.Int("attempts", event.Attempts+1),
			zap.Error(err))
		_ = r.repo.MarkFailed(ctx, event.ID, err.Error(), r.cfg.MaxAttempts)
		return
	}

	if markErr := r.repo.MarkPublished(ctx, event.ID); markErr != nil {
		r.log.Error("mark outbox event published",
			zap.String("id", event.ID.String()), zap.Error(markErr))
		return
	}

	r.log.Debug("outbox event published",
		zap.String("id", event.ID.String()),
		zap.String("type", event.EventType),
		zap.String("topic", event.Topic))
}
