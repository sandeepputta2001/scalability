// consistent_hash.go implements a virtual-node consistent hash ring.
//
// Why virtual nodes?
//
//	Plain modulo hashing (FNV % N) causes massive data movement when shards
//	are added/removed — every key re-hashes to a different shard.
//	A ring with V virtual nodes per shard means only K/N keys move on average
//	when a shard is added (K = total keys, N = shard count).
//
// Tradeoff: more memory for the ring, slightly slower lookup (binary search
// on sorted ring vs. single modulo), but dramatically better rebalancing.
package db

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

const defaultVirtualNodes = 150 // 150 vnodes per shard gives good distribution

// ringEntry maps a hash position on the ring to a shard ID.
type ringEntry struct {
	hash    uint32
	shardID int
}

// ConsistentHashRing is a thread-safe virtual-node consistent hash ring.
type ConsistentHashRing struct {
	mu           sync.RWMutex
	ring         []ringEntry // sorted by hash
	virtualNodes int
}

// NewConsistentHashRing builds a ring from the given shard IDs.
func NewConsistentHashRing(shardIDs []int, virtualNodes int) *ConsistentHashRing {
	if virtualNodes <= 0 {
		virtualNodes = defaultVirtualNodes
	}
	r := &ConsistentHashRing{virtualNodes: virtualNodes}
	for _, id := range shardIDs {
		r.addShard(id)
	}
	return r
}

// addShard inserts virtualNodes entries for the given shard into the ring.
func (r *ConsistentHashRing) addShard(shardID int) {
	for i := 0; i < r.virtualNodes; i++ {
		key := fmt.Sprintf("shard-%d-vnode-%d", shardID, i)
		h := hashKey(key)
		r.ring = append(r.ring, ringEntry{hash: h, shardID: shardID})
	}
	sort.Slice(r.ring, func(i, j int) bool {
		return r.ring[i].hash < r.ring[j].hash
	})
}

// GetShardID returns the shard ID responsible for the given key.
// Uses binary search on the sorted ring — O(log N*V).
func (r *ConsistentHashRing) GetShardID(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 {
		return 0
	}
	h := hashKey(key)
	// Find first ring entry with hash >= h (clockwise successor)
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].hash >= h
	})
	// Wrap around to the first entry if we're past the end
	if idx == len(r.ring) {
		idx = 0
	}
	return r.ring[idx].shardID
}

// Distribution returns how many virtual nodes each shard owns.
// Useful for diagnosing uneven distribution.
func (r *ConsistentHashRing) Distribution() map[int]int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	dist := make(map[int]int)
	for _, e := range r.ring {
		dist[e.shardID]++
	}
	return dist
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
