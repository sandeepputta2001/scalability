package kafka

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// EventType is the discriminator field in every event envelope.
type EventType string

const (
	EventOrderCreated   EventType = "order.created"
	EventOrderConfirmed EventType = "order.confirmed"
	EventOrderShipped   EventType = "order.shipped"
	EventOrderCancelled EventType = "order.cancelled"
	EventStockUpdated   EventType = "stock.updated"
	EventUserRegistered EventType = "user.registered"
)

// Envelope wraps every domain event with metadata needed for:
//   - Idempotency:  EventID is a UUID; consumers deduplicate by EventID
//   - Ordering:     SequenceNum is monotonically increasing per aggregate
//   - Tracing:      CorrelationID links events across services
//   - Schema:       Version allows consumers to handle schema evolution
type Envelope struct {
	EventID       string    `json:"event_id"` // UUID, used for deduplication
	EventType     EventType `json:"event_type"`
	AggregateID   string    `json:"aggregate_id"`   // order_id / user_id / product_id
	AggregateType string    `json:"aggregate_type"` // "order" | "user" | "product"
	CorrelationID string    `json:"correlation_id"` // request trace ID
	SequenceNum   int64     `json:"sequence_num"`   // monotonic per aggregate
	Version       int       `json:"version"`        // schema version
	OccurredAt    time.Time `json:"occurred_at"`
	Payload       []byte    `json:"payload"` // JSON-encoded domain event
}

// NewEnvelope creates a new event envelope with a fresh EventID.
func NewEnvelope(eventType EventType, aggregateID, aggregateType, correlationID string, payload any) (*Envelope, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	return &Envelope{
		EventID:       uuid.New().String(),
		EventType:     eventType,
		AggregateID:   aggregateID,
		AggregateType: aggregateType,
		CorrelationID: correlationID,
		Version:       1,
		OccurredAt:    time.Now().UTC(),
		Payload:       data,
	}, nil
}

// Encode serialises the envelope to JSON bytes for the Kafka message value.
func (e *Envelope) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// DecodeEnvelope deserialises a Kafka message value into an Envelope.
func DecodeEnvelope(data []byte) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(data, &e); err != nil {
		return nil, fmt.Errorf("decode envelope: %w", err)
	}
	return &e, nil
}

// ─── Domain event payloads ────────────────────────────────────────────────────

type OrderCreatedEvent struct {
	OrderID    string      `json:"order_id"`
	UserID     string      `json:"user_id"`
	TotalPrice float64     `json:"total_price"`
	Items      []OrderItem `json:"items"`
	ShardKey   int         `json:"shard_key"`
}

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	UnitPrice float64 `json:"unit_price"`
}

type OrderStatusChangedEvent struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	OldStatus string `json:"old_status"`
	NewStatus string `json:"new_status"`
	ShardKey  int    `json:"shard_key"`
}

type StockUpdatedEvent struct {
	ProductID string `json:"product_id"`
	Delta     int    `json:"delta"` // negative = deduction
	NewStock  int    `json:"new_stock"`
}

type UserRegisteredEvent struct {
	UserID   string `json:"user_id"`
	Email    string `json:"email"`
	Name     string `json:"name"`
	ShardKey int    `json:"shard_key"`
}
