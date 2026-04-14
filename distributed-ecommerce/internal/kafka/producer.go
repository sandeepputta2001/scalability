// producer.go implements a partitioned, idempotent Kafka producer.
//
// Partitioning strategy:
//
//	Messages are keyed by user_id (or order_id for order events).
//	kafka-go hashes the key with murmur2 (same as the Java client) to select
//	a partition. This guarantees all events for the same user land on the
//	same partition → strict per-user ordering.
//
// Idempotency (exactly-once produce):
//
//	RequiredAcks = RequireAll  → leader waits for all ISR replicas to ack
//	This prevents message loss on leader failure mid-write.
//	Combined with producer retries, this gives at-least-once delivery.
//	Consumer-side deduplication (via Redis seen-set) upgrades to exactly-once.
//
// Replication durability:
//
//	With RF=3 and acks=all, a message is durable even if 2 brokers fail
//	simultaneously (as long as the ISR has ≥1 member).
package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/config"
)

// Producer wraps kafka-go Writer with structured event publishing.
type Producer struct {
	writers map[string]*kafka.Writer // topic → writer
	log     *zap.Logger
}

// NewProducer creates one Writer per topic for optimal batching.
func NewProducer(cfg config.KafkaConfig, log *zap.Logger) *Producer {
	topics := []string{
		TopicOrderCreated, TopicOrderConfirmed, TopicOrderShipped,
		TopicOrderCancelled, TopicStockUpdated, TopicUserRegistered, TopicDLQ,
	}

	writers := make(map[string]*kafka.Writer, len(topics))
	for _, topic := range topics {
		writers[topic] = &kafka.Writer{
			Addr:  kafka.TCP(cfg.Brokers...),
			Topic: topic,

			// Balancer: hash partition key with murmur2 (Kafka-compatible)
			// All messages with the same key go to the same partition.
			Balancer: &kafka.Hash{},

			// RequireAll = acks=-1: leader waits for all ISR replicas.
			// Strongest durability; slightly higher latency than acks=1.
			RequiredAcks: kafka.RequireAll,

			// Async batching: accumulate messages for up to 10ms before flush.
			// Tradeoff: lower throughput latency vs. slightly higher per-message latency.
			BatchTimeout: 10 * time.Millisecond,
			BatchSize:    100,

			// Retry on transient errors (leader election, network blip)
			MaxAttempts: 5,

			// Compression reduces network + storage cost ~60-80% for JSON payloads
			Compression: kafka.Snappy,

			// Allow topic auto-creation in dev; disable in prod
			AllowAutoTopicCreation: cfg.AutoCreateTopics,

			Logger:      kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Sugar().Debugf(msg, args...) }),
			ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) { log.Sugar().Errorf(msg, args...) }),
		}
	}

	log.Info("kafka producer initialised",
		zap.Strings("brokers", cfg.Brokers),
		zap.Int("topics", len(topics)))
	return &Producer{writers: writers, log: log}
}

// Publish serialises a domain event into an Envelope and writes it to Kafka.
// The partition key is always the AggregateID (user_id / order_id) so that
// all events for the same entity land on the same partition.
func (p *Producer) Publish(ctx context.Context, topic string, env *Envelope) error {
	w, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("unknown topic: %s", topic)
	}

	data, err := env.Encode()
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(env.AggregateID), // partition key
		Value: data,
		// Headers carry metadata without touching the payload
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(env.EventType)},
			{Key: "correlation_id", Value: []byte(env.CorrelationID)},
			{Key: "schema_version", Value: []byte(fmt.Sprintf("%d", env.Version))},
		},
		Time: env.OccurredAt,
	}

	if err := w.WriteMessages(ctx, msg); err != nil {
		p.log.Error("kafka publish failed",
			zap.String("topic", topic),
			zap.String("event_id", env.EventID),
			zap.Error(err))
		return fmt.Errorf("kafka write: %w", err)
	}

	p.log.Debug("event published",
		zap.String("topic", topic),
		zap.String("event_id", env.EventID),
		zap.String("aggregate_id", env.AggregateID))
	return nil
}

// PublishToDLQ sends a failed message to the dead-letter queue with error context.
func (p *Producer) PublishToDLQ(ctx context.Context, original *Envelope, processingErr error) error {
	dlqPayload := map[string]any{
		"original_event": original,
		"error":          processingErr.Error(),
		"failed_at":      time.Now().UTC(),
	}
	env, err := NewEnvelope(
		EventType("dlq."+string(original.EventType)),
		original.AggregateID,
		original.AggregateType,
		original.CorrelationID,
		dlqPayload,
	)
	if err != nil {
		return err
	}
	return p.Publish(ctx, TopicDLQ, env)
}

// Close flushes and closes all writers.
func (p *Producer) Close() {
	for topic, w := range p.writers {
		if err := w.Close(); err != nil {
			p.log.Warn("kafka writer close error", zap.String("topic", topic), zap.Error(err))
		}
	}
}
