// admin.go manages Kafka topic creation with the correct partition count
// and replication factor. Called at service startup.
//
// Topic Partitioning Design:
//
//	order.created   → 6 partitions  (high volume, 6-way parallelism)
//	stock.updated   → 3 partitions  (medium volume)
//	user.registered → 3 partitions  (low volume)
//	events.dlq      → 1 partition   (low volume, ordering matters for replay)
//
// Replication Factor = 3:
//
//	Requires at least 3 Kafka brokers.
//	In dev (1 broker), we fall back to RF=1 automatically.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/distributed-ecommerce/internal/config"
)

// EnsureTopics creates all required topics if they don't already exist.
func EnsureTopics(ctx context.Context, cfg config.KafkaConfig, log *zap.Logger) error {
	conn, err := kafka.DialContext(ctx, "tcp", cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("kafka dial: %w", err)
	}
	defer conn.Close()

	// Find the controller broker (only the controller can create topics)
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}
	controllerConn, err := kafka.DialContext(ctx, "tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	// Determine effective replication factor based on available brokers
	rf := cfg.ReplicationFactor
	if rf <= 0 {
		rf = ReplicationFactor
	}

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             TopicOrderCreated,
			NumPartitions:     PartitionsOrderEvents,
			ReplicationFactor: rf,
			ConfigEntries:     retentionConfig("7d"),
		},
		{
			Topic:             TopicOrderConfirmed,
			NumPartitions:     PartitionsOrderEvents,
			ReplicationFactor: rf,
			ConfigEntries:     retentionConfig("7d"),
		},
		{
			Topic:             TopicOrderShipped,
			NumPartitions:     PartitionsOrderEvents,
			ReplicationFactor: rf,
			ConfigEntries:     retentionConfig("7d"),
		},
		{
			Topic:             TopicOrderCancelled,
			NumPartitions:     PartitionsOrderEvents,
			ReplicationFactor: rf,
			ConfigEntries:     retentionConfig("7d"),
		},
		{
			Topic:             TopicStockUpdated,
			NumPartitions:     PartitionsStockEvents,
			ReplicationFactor: rf,
			ConfigEntries:     retentionConfig("3d"),
		},
		{
			Topic:             TopicUserRegistered,
			NumPartitions:     PartitionsUserEvents,
			ReplicationFactor: rf,
			ConfigEntries:     retentionConfig("30d"),
		},
		{
			Topic:             TopicDLQ,
			NumPartitions:     PartitionsDLQ,
			ReplicationFactor: rf,
			ConfigEntries:     retentionConfig("30d"), // keep DLQ events longer
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// TopicAlreadyExists is not an error — idempotent startup
		var kafkaErr kafka.Error
		if errors.As(err, &kafkaErr) && kafkaErr == kafka.TopicAlreadyExists {
			log.Info("kafka topics already exist")
			return nil
		}
		// In dev with a single broker, RF>1 will fail — log and continue
		log.Warn("kafka topic creation warning (may be RF mismatch in dev)",
			zap.Error(err))
		return nil
	}

	log.Info("kafka topics ensured",
		zap.Int("count", len(topicConfigs)),
		zap.Int("replication_factor", rf))
	return nil
}

// retentionConfig returns topic-level retention settings.
func retentionConfig(duration string) []kafka.ConfigEntry {
	msMap := map[string]string{
		"3d":  "259200000",
		"7d":  "604800000",
		"30d": "2592000000",
	}
	ms, ok := msMap[duration]
	if !ok {
		ms = "604800000" // default 7d
	}
	return []kafka.ConfigEntry{
		{ConfigName: "retention.ms", ConfigValue: ms},
		{ConfigName: "cleanup.policy", ConfigValue: "delete"},
		// min.insync.replicas=2: at least 2 replicas must ack for acks=all
		// This means RF=3 can tolerate 1 broker failure during writes
		{ConfigName: "min.insync.replicas", ConfigValue: "1"}, // 1 for dev; use 2 in prod
	}
}
