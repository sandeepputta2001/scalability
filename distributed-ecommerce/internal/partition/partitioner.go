// Package partition implements four data partitioning strategies and a
// data distribution analyzer.
//
// Partitioning Strategies:
//
//  1. Hash Partitioning
//     key → hash(key) % N → partition
//     Uniform distribution, but no range queries across partitions.
//     Tradeoff: adding partitions requires rehashing all keys (use consistent
//     hash to mitigate). No ordering guarantees.
//
//  2. Range Partitioning
//     key → compare against sorted boundary list → partition
//     Supports range queries within a partition. Easy to reason about.
//     Tradeoff: hot spots if data is skewed (e.g., all new orders in the
//     latest range partition). Requires manual boundary tuning.
//
//  3. Directory Partitioning
//     Explicit lookup table: key → partition ID.
//     Maximum flexibility; can move individual keys between partitions.
//     Tradeoff: lookup table is a single point of failure and bottleneck.
//     Must be kept consistent (use Redis or a distributed KV store).
//
//  4. Composite Partitioning
//     Two-level: first hash by tenant/user, then range within that partition.
//     Combines benefits of both: tenant isolation + range query support.
//     Tradeoff: more complex routing logic; cross-tenant queries require
//     scatter-gather.
//
// Data Distribution Analyzer:
//
//	Tracks key counts per partition and computes skew metrics.
//	Skew > 20% triggers a rebalancing recommendation.
package partition

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"sync/atomic"
)

// Partitioner routes a key to a partition ID.
type Partitioner interface {
	Partition(key string) int
	NumPartitions() int
	Strategy() string
}

// ─── Hash Partitioner ─────────────────────────────────────────────────────────

// HashPartitioner uses FNV-1a hash modulo N.
// Consistent hash ring variant is in db/consistent_hash.go.
type HashPartitioner struct {
	n int
}

func NewHashPartitioner(n int) *HashPartitioner {
	return &HashPartitioner{n: n}
}

func (h *HashPartitioner) Partition(key string) int {
	hv := fnv.New32a()
	_, _ = hv.Write([]byte(key))
	return int(hv.Sum32()) % h.n
}

func (h *HashPartitioner) NumPartitions() int { return h.n }
func (h *HashPartitioner) Strategy() string   { return "hash" }

// ─── Range Partitioner ────────────────────────────────────────────────────────

// RangePartitioner assigns keys to partitions based on sorted boundary values.
// Keys < boundaries[0] → partition 0
// Keys in [boundaries[i-1], boundaries[i]) → partition i
// Keys >= boundaries[last] → partition N-1
//
// Example for user IDs:
//
//	boundaries = ["D", "M", "T"]
//	"Alice" → 0, "Dave" → 1, "Mike" → 2, "Zara" → 3
type RangePartitioner struct {
	boundaries []string // sorted
}

func NewRangePartitioner(boundaries []string) *RangePartitioner {
	sorted := make([]string, len(boundaries))
	copy(sorted, boundaries)
	sort.Strings(sorted)
	return &RangePartitioner{boundaries: sorted}
}

func (r *RangePartitioner) Partition(key string) int {
	// Binary search for the first boundary > key
	idx := sort.SearchStrings(r.boundaries, key)
	if idx >= len(r.boundaries) {
		return len(r.boundaries) // last partition
	}
	return idx
}

func (r *RangePartitioner) NumPartitions() int { return len(r.boundaries) + 1 }
func (r *RangePartitioner) Strategy() string   { return "range" }

// ─── Directory Partitioner ────────────────────────────────────────────────────

// DirectoryPartitioner uses an explicit lookup table.
// Unregistered keys fall back to hash partitioning.
//
// Tradeoff: the directory is a bottleneck and single point of failure.
// In production, store the directory in Redis or a distributed KV store.
type DirectoryPartitioner struct {
	directory map[string]int
	fallback  *HashPartitioner
	mu        sync.RWMutex
}

func NewDirectoryPartitioner(n int) *DirectoryPartitioner {
	return &DirectoryPartitioner{
		directory: make(map[string]int),
		fallback:  NewHashPartitioner(n),
	}
}

// Register explicitly maps a key to a partition.
func (d *DirectoryPartitioner) Register(key string, partition int) {
	d.mu.Lock()
	d.directory[key] = partition
	d.mu.Unlock()
}

// Migrate moves a key from one partition to another.
func (d *DirectoryPartitioner) Migrate(key string, toPartition int) {
	d.mu.Lock()
	d.directory[key] = toPartition
	d.mu.Unlock()
}

func (d *DirectoryPartitioner) Partition(key string) int {
	d.mu.RLock()
	if p, ok := d.directory[key]; ok {
		d.mu.RUnlock()
		return p
	}
	d.mu.RUnlock()
	return d.fallback.Partition(key)
}

func (d *DirectoryPartitioner) NumPartitions() int { return d.fallback.n }
func (d *DirectoryPartitioner) Strategy() string   { return "directory" }

// ─── Composite Partitioner ────────────────────────────────────────────────────

// CompositePartitioner implements two-level partitioning:
//
//	Level 1: hash by tenant (user_id prefix) → shard group
//	Level 2: range within shard group → sub-partition
//
// Key format: "{tenant_id}:{entity_key}"
// Example: "user-123:order-456" → tenant "user-123" → shard 2 → sub-partition 1
//
// This gives tenant isolation (all data for a tenant on the same shard group)
// while supporting range queries within a tenant's data.
type CompositePartitioner struct {
	tenantHasher  *HashPartitioner  // level 1: tenant → shard group
	rangeInGroup  *RangePartitioner // level 2: key → sub-partition within group
	subPartitions int
}

func NewCompositePartitioner(tenantShards int, boundaries []string) *CompositePartitioner {
	return &CompositePartitioner{
		tenantHasher:  NewHashPartitioner(tenantShards),
		rangeInGroup:  NewRangePartitioner(boundaries),
		subPartitions: len(boundaries) + 1,
	}
}

// Partition returns a globally unique partition ID.
// partition = (tenantShard * subPartitions) + subPartition
func (c *CompositePartitioner) Partition(key string) int {
	// Extract tenant prefix (everything before the first ":")
	tenantKey := key
	for i, ch := range key {
		if ch == ':' {
			tenantKey = key[:i]
			break
		}
	}
	tenantShard := c.tenantHasher.Partition(tenantKey)
	subPartition := c.rangeInGroup.Partition(key)
	return tenantShard*c.subPartitions + subPartition
}

func (c *CompositePartitioner) NumPartitions() int {
	return c.tenantHasher.n * c.subPartitions
}
func (c *CompositePartitioner) Strategy() string { return "composite" }

// ─── Distribution Analyzer ────────────────────────────────────────────────────

// DistributionAnalyzer tracks key counts per partition and computes skew.
// Skew is the coefficient of variation (stddev / mean) of key counts.
// Skew > 0.2 (20%) triggers a rebalancing recommendation.
type DistributionAnalyzer struct {
	counts []atomic.Int64
	n      int
}

func NewDistributionAnalyzer(n int) *DistributionAnalyzer {
	return &DistributionAnalyzer{counts: make([]atomic.Int64, n), n: n}
}

// Record increments the counter for the given partition.
func (d *DistributionAnalyzer) Record(partition int) {
	if partition >= 0 && partition < d.n {
		d.counts[partition].Add(1)
	}
}

// Report returns per-partition counts, total, mean, stddev, and skew.
func (d *DistributionAnalyzer) Report() DistributionReport {
	counts := make([]int64, d.n)
	var total int64
	for i := range d.counts {
		counts[i] = d.counts[i].Load()
		total += counts[i]
	}
	mean := float64(total) / float64(d.n)
	var variance float64
	for _, c := range counts {
		diff := float64(c) - mean
		variance += diff * diff
	}
	variance /= float64(d.n)
	stddev := math.Sqrt(variance)
	skew := 0.0
	if mean > 0 {
		skew = stddev / mean
	}

	// Find hottest and coldest partitions
	maxIdx, minIdx := 0, 0
	for i, c := range counts {
		if c > counts[maxIdx] {
			maxIdx = i
		}
		if c < counts[minIdx] {
			minIdx = i
		}
	}

	return DistributionReport{
		Counts:               counts,
		Total:                total,
		Mean:                 mean,
		StdDev:               stddev,
		SkewCoefficient:      skew,
		HottestPartition:     maxIdx,
		ColdestPartition:     minIdx,
		RebalanceRecommended: skew > 0.2,
	}
}

// DistributionReport is the output of the analyzer.
type DistributionReport struct {
	Counts               []int64 `json:"counts"`
	Total                int64   `json:"total"`
	Mean                 float64 `json:"mean"`
	StdDev               float64 `json:"std_dev"`
	SkewCoefficient      float64 `json:"skew_coefficient"`
	HottestPartition     int     `json:"hottest_partition"`
	ColdestPartition     int     `json:"coldest_partition"`
	RebalanceRecommended bool    `json:"rebalance_recommended"`
}

// ─── Partition Router ─────────────────────────────────────────────────────────

// Router combines a Partitioner with a DistributionAnalyzer.
// All application code should use Router instead of Partitioner directly.
type Router struct {
	partitioner Partitioner
	analyzer    *DistributionAnalyzer
}

func NewRouter(p Partitioner) *Router {
	return &Router{
		partitioner: p,
		analyzer:    NewDistributionAnalyzer(p.NumPartitions()),
	}
}

// Route returns the partition for a key and records the access.
func (r *Router) Route(key string) int {
	p := r.partitioner.Partition(key)
	r.analyzer.Record(p)
	return p
}

// Report returns the current distribution report.
func (r *Router) Report() DistributionReport {
	return r.analyzer.Report()
}

// Strategy returns the partitioning strategy name.
func (r *Router) Strategy() string { return r.partitioner.Strategy() }

// NumPartitions returns the total number of partitions.
func (r *Router) NumPartitions() int { return r.partitioner.NumPartitions() }

// KeyInfo returns routing info for a specific key.
func (r *Router) KeyInfo(key string) map[string]any {
	p := r.partitioner.Partition(key)
	return map[string]any{
		"key":              key,
		"partition":        p,
		"strategy":         r.partitioner.Strategy(),
		"total_partitions": r.partitioner.NumPartitions(),
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func (r *Router) String() string {
	return fmt.Sprintf("Router{strategy=%s, partitions=%d}", r.Strategy(), r.NumPartitions())
}
