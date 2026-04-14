// Package kafka centralises all Kafka topic names, partition counts,
// replication factors, and event schema definitions.
//
// Kafka Sharding = Partitions
//
//	A Kafka topic is split into N partitions. Each partition is an ordered,
//	immutable log. Producers choose a partition via a partition key (same key
//	always lands on the same partition — ordering guarantee per key).
//
//	Partition count = degree of parallelism for consumers.
//	More partitions → more consumer instances can read in parallel.
//
// Kafka Replication
//
//	Each partition has 1 leader + (RF-1) follower replicas on different brokers.
//	Producers write to the leader; followers replicate asynchronously.
//	acks=all (acks=-1) means the leader waits for ALL in-sync replicas (ISR)
//	to acknowledge before confirming the write — strongest durability guarantee.
//
// Consistency via Idempotent Producer + Exactly-Once Semantics
//
//	enable.idempotence=true: broker deduplicates retried produce requests
//	using (ProducerID, SequenceNumber). Prevents duplicate messages on retry.
//
// Consumer Groups = Horizontal Scaling
//
//	Each consumer group gets its own independent offset cursor per partition.
//	Adding consumers to a group triggers a rebalance — partitions are
//	redistributed so each partition is consumed by exactly one member.
package kafka

// Topic names
const (
	TopicOrderCreated   = "order.created"   // fired when order is placed
	TopicOrderConfirmed = "order.confirmed" // fired when payment confirmed
	TopicOrderShipped   = "order.shipped"   // fired when order dispatched
	TopicOrderCancelled = "order.cancelled" // fired on cancellation
	TopicStockUpdated   = "stock.updated"   // fired after stock deduction
	TopicUserRegistered = "user.registered" // fired on new user signup
	TopicDLQ            = "events.dlq"      // dead-letter queue for failed events
)

// Partition counts — must match broker topic configuration.
// Rule of thumb: partitions = max expected consumer parallelism.
const (
	PartitionsOrderEvents = 6 // 6 consumers can process orders in parallel
	PartitionsStockEvents = 3
	PartitionsUserEvents  = 3
	PartitionsDLQ         = 1
)

// ReplicationFactor for all topics.
// RF=3 means 1 leader + 2 followers. Tolerates 2 broker failures.
// Minimum viable: RF=2 (tolerates 1 failure). RF=1 = no fault tolerance.
const ReplicationFactor = 3

// Consumer group IDs — each group maintains independent offsets.
const (
	GroupOrderProcessor   = "order-processor"   // processes order lifecycle
	GroupStockProcessor   = "stock-processor"   // updates inventory
	GroupNotificationSvc  = "notification-svc"  // sends emails/push
	GroupAnalyticsSvc     = "analytics-svc"     // writes to data warehouse
	GroupCacheInvalidator = "cache-invalidator" // invalidates Redis on events
)

// PartitionKeyFor returns the Kafka partition key for a given entity.
// Using user_id as partition key ensures all events for a user land on the
// same partition → strict ordering of events per user.
func PartitionKeyFor(userID string) []byte {
	return []byte(userID)
}
