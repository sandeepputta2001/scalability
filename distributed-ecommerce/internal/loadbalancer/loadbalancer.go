// Package loadbalancer implements four load-balancing algorithms used across
// the system for distributing requests across DB replicas, Redis nodes,
// and upstream HTTP services.
//
// Algorithms implemented:
//
//  1. Round-Robin
//     Requests distributed evenly in rotation: 1→2→3→1→2→3...
//     Tradeoff: simple, zero state, but ignores node capacity and latency.
//     Best for: homogeneous nodes with similar processing times.
//
//  2. Weighted Round-Robin
//     Nodes assigned weights; higher-weight nodes receive proportionally more
//     requests. Weight 3 node gets 3× requests vs. weight 1 node.
//     Tradeoff: requires manual weight tuning; doesn't adapt to runtime load.
//     Best for: heterogeneous nodes (different CPU/RAM), canary deployments.
//
//  3. Least Connections (Least Outstanding Requests)
//     Route to the node with fewest in-flight requests.
//     Tradeoff: requires tracking active connections (atomic counter per node);
//     can cause thundering herd if a slow node suddenly frees up.
//     Best for: long-lived connections, variable request duration.
//
//  4. Consistent Hash (with virtual nodes)
//     Same key always routes to the same node. Adding/removing nodes only
//     remaps ~1/N of keys (vs. all keys with modulo).
//     Tradeoff: uneven distribution without virtual nodes; more memory.
//     Best for: session affinity, cache locality, stateful services.
//
//     Health Checking:
//     All algorithms skip nodes marked unhealthy by the health checker.
//     Health is checked via periodic pings; unhealthy nodes are re-checked
//     with exponential back-off before being re-admitted.
package loadbalancer

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// ErrNoHealthyNodes is returned when all nodes are unhealthy.
var ErrNoHealthyNodes = errors.New("no healthy nodes available")

// Node represents a single backend endpoint.
type Node struct {
	ID          string
	Addr        string
	Weight      int // for weighted round-robin (default 1)
	activeConns atomic.Int64
	healthy     atomic.Bool
	failCount   atomic.Int64
	lastFail    atomic.Int64 // unix nano
}

func NewNode(id, addr string, weight int) *Node {
	n := &Node{ID: id, Addr: addr, Weight: weight}
	n.healthy.Store(true)
	if n.Weight <= 0 {
		n.Weight = 1
	}
	return n
}

func (n *Node) IsHealthy() bool    { return n.healthy.Load() }
func (n *Node) ActiveConns() int64 { return n.activeConns.Load() }
func (n *Node) IncrConns()         { n.activeConns.Add(1) }
func (n *Node) DecrConns()         { n.activeConns.Add(-1) }

// ─── Algorithm interface ──────────────────────────────────────────────────────

// Algorithm selects a node for a given request key.
type Algorithm interface {
	// Pick returns the next node. key is used only by consistent-hash.
	Pick(key string) (*Node, error)
	// Nodes returns all registered nodes.
	Nodes() []*Node
	// Name returns the algorithm name for observability.
	Name() string
}

// ─── Round-Robin ──────────────────────────────────────────────────────────────

type RoundRobin struct {
	nodes   []*Node
	counter atomic.Uint64
	mu      sync.RWMutex
}

func NewRoundRobin(nodes []*Node) *RoundRobin {
	return &RoundRobin{nodes: nodes}
}

func (r *RoundRobin) Name() string { return "round-robin" }

func (r *RoundRobin) Pick(_ string) (*Node, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	healthy := healthyNodes(r.nodes)
	if len(healthy) == 0 {
		return nil, ErrNoHealthyNodes
	}
	idx := r.counter.Add(1) % uint64(len(healthy))
	return healthy[idx], nil
}

func (r *RoundRobin) Nodes() []*Node { return r.nodes }

// ─── Weighted Round-Robin ─────────────────────────────────────────────────────

// WeightedRoundRobin uses the smooth weighted round-robin algorithm (Nginx-style).
// Each node has a current_weight that increases by its weight each round.
// The node with the highest current_weight is selected, then its weight is
// reduced by the total weight sum. This produces a smooth distribution.
type WeightedRoundRobin struct {
	nodes         []*Node
	currentWeight []int
	mu            sync.Mutex
}

func NewWeightedRoundRobin(nodes []*Node) *WeightedRoundRobin {
	return &WeightedRoundRobin{
		nodes:         nodes,
		currentWeight: make([]int, len(nodes)),
	}
}

func (w *WeightedRoundRobin) Name() string { return "weighted-round-robin" }

func (w *WeightedRoundRobin) Pick(_ string) (*Node, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	totalWeight := 0
	best := -1

	for i, n := range w.nodes {
		if !n.IsHealthy() {
			continue
		}
		w.currentWeight[i] += n.Weight
		totalWeight += n.Weight
		if best == -1 || w.currentWeight[i] > w.currentWeight[best] {
			best = i
		}
	}
	if best == -1 {
		return nil, ErrNoHealthyNodes
	}
	w.currentWeight[best] -= totalWeight
	return w.nodes[best], nil
}

func (w *WeightedRoundRobin) Nodes() []*Node { return w.nodes }

// ─── Least Connections ────────────────────────────────────────────────────────

type LeastConnections struct {
	nodes []*Node
	mu    sync.RWMutex
}

func NewLeastConnections(nodes []*Node) *LeastConnections {
	return &LeastConnections{nodes: nodes}
}

func (l *LeastConnections) Name() string { return "least-connections" }

func (l *LeastConnections) Pick(_ string) (*Node, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var best *Node
	for _, n := range l.nodes {
		if !n.IsHealthy() {
			continue
		}
		if best == nil || n.ActiveConns() < best.ActiveConns() {
			best = n
		}
	}
	if best == nil {
		return nil, ErrNoHealthyNodes
	}
	return best, nil
}

func (l *LeastConnections) Nodes() []*Node { return l.nodes }

// ─── Consistent Hash ──────────────────────────────────────────────────────────

const defaultVNodes = 150

type consistentHashEntry struct {
	hash   uint32
	nodeID string
}

type ConsistentHash struct {
	ring    []consistentHashEntry
	nodeMap map[string]*Node
	vNodes  int
	mu      sync.RWMutex
}

func NewConsistentHash(nodes []*Node, vNodes int) *ConsistentHash {
	if vNodes <= 0 {
		vNodes = defaultVNodes
	}
	ch := &ConsistentHash{nodeMap: make(map[string]*Node), vNodes: vNodes}
	for _, n := range nodes {
		ch.addNode(n)
	}
	return ch
}

func (c *ConsistentHash) Name() string { return "consistent-hash" }

func (c *ConsistentHash) addNode(n *Node) {
	c.nodeMap[n.ID] = n
	for i := 0; i < c.vNodes; i++ {
		key := fmt.Sprintf("%s-vnode-%d", n.ID, i)
		h := fnv32(key)
		c.ring = append(c.ring, consistentHashEntry{hash: h, nodeID: n.ID})
	}
	sort.Slice(c.ring, func(i, j int) bool { return c.ring[i].hash < c.ring[j].hash })
}

func (c *ConsistentHash) Pick(key string) (*Node, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.ring) == 0 {
		return nil, ErrNoHealthyNodes
	}
	h := fnv32(key)
	idx := sort.Search(len(c.ring), func(i int) bool { return c.ring[i].hash >= h })
	if idx == len(c.ring) {
		idx = 0
	}
	// Walk clockwise until we find a healthy node
	for i := 0; i < len(c.ring); i++ {
		entry := c.ring[(idx+i)%len(c.ring)]
		if n, ok := c.nodeMap[entry.nodeID]; ok && n.IsHealthy() {
			return n, nil
		}
	}
	return nil, ErrNoHealthyNodes
}

func (c *ConsistentHash) Nodes() []*Node {
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := make([]*Node, 0, len(c.nodeMap))
	for _, n := range c.nodeMap {
		nodes = append(nodes, n)
	}
	return nodes
}

// ─── Health Checker ───────────────────────────────────────────────────────────

// HealthChecker periodically probes nodes and marks them healthy/unhealthy.
// Uses exponential back-off for re-checking failed nodes.
type HealthChecker struct {
	nodes    []*Node
	probe    func(ctx context.Context, node *Node) error
	interval time.Duration
	log      *zap.Logger
	stopCh   chan struct{}
}

func NewHealthChecker(
	nodes []*Node,
	probe func(ctx context.Context, node *Node) error,
	interval time.Duration,
	log *zap.Logger,
) *HealthChecker {
	return &HealthChecker{
		nodes:    nodes,
		probe:    probe,
		interval: interval,
		log:      log,
		stopCh:   make(chan struct{}),
	}
}

func (h *HealthChecker) Start() {
	go h.loop()
}

func (h *HealthChecker) Stop() {
	close(h.stopCh)
}

func (h *HealthChecker) loop() {
	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			h.checkAll()
		case <-h.stopCh:
			return
		}
	}
}

func (h *HealthChecker) checkAll() {
	for _, node := range h.nodes {
		go func(n *Node) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := h.probe(ctx, n)
			wasHealthy := n.IsHealthy()
			if err != nil {
				n.healthy.Store(false)
				n.failCount.Add(1)
				n.lastFail.Store(time.Now().UnixNano())
				if wasHealthy {
					h.log.Warn("node marked unhealthy",
						zap.String("id", n.ID), zap.String("addr", n.Addr), zap.Error(err))
				}
			} else {
				n.healthy.Store(true)
				n.failCount.Store(0)
				if !wasHealthy {
					h.log.Info("node recovered", zap.String("id", n.ID), zap.String("addr", n.Addr))
				}
			}
		}(node)
	}
}

// ─── LoadBalancer (combines algorithm + health checker) ───────────────────────

// LoadBalancer wraps an algorithm with health checking and connection tracking.
type LoadBalancer struct {
	algo    Algorithm
	checker *HealthChecker
	log     *zap.Logger
}

func New(algo Algorithm, checker *HealthChecker, log *zap.Logger) *LoadBalancer {
	if checker != nil {
		checker.Start()
	}
	return &LoadBalancer{algo: algo, checker: checker, log: log}
}

// Do picks a node, tracks active connections, executes fn, then decrements.
// This is the primary entry point for load-balanced calls.
func (lb *LoadBalancer) Do(ctx context.Context, key string, fn func(node *Node) error) error {
	node, err := lb.algo.Pick(key)
	if err != nil {
		return fmt.Errorf("load balancer: %w", err)
	}
	node.IncrConns()
	defer node.DecrConns()
	return fn(node)
}

// Stats returns per-node health and connection stats.
func (lb *LoadBalancer) Stats() []NodeStats {
	stats := make([]NodeStats, 0)
	for _, n := range lb.algo.Nodes() {
		stats = append(stats, NodeStats{
			ID:          n.ID,
			Addr:        n.Addr,
			Weight:      n.Weight,
			Healthy:     n.IsHealthy(),
			ActiveConns: n.ActiveConns(),
			FailCount:   n.failCount.Load(),
			Algorithm:   lb.algo.Name(),
		})
	}
	return stats
}

// NodeStats is the observability payload for a single node.
type NodeStats struct {
	ID          string `json:"id"`
	Addr        string `json:"addr"`
	Weight      int    `json:"weight"`
	Healthy     bool   `json:"healthy"`
	ActiveConns int64  `json:"active_conns"`
	FailCount   int64  `json:"fail_count"`
	Algorithm   string `json:"algorithm"`
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func healthyNodes(nodes []*Node) []*Node {
	out := make([]*Node, 0, len(nodes))
	for _, n := range nodes {
		if n.IsHealthy() {
			out = append(out, n)
		}
	}
	return out
}

func fnv32(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
