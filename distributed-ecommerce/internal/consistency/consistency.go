// Package consistency implements four consistency levels that can be requested
// per-operation via context, and enforces them across PostgreSQL, MongoDB,
// and Redis reads/writes.
//
// Consistency Levels (weakest → strongest):
//
//  1. Eventual Consistency
//     Reads may return stale data. Writes acknowledged by one node.
//     System converges to consistent state eventually.
//     Tradeoff: lowest latency, highest availability (AP in CAP theorem).
//     Use for: product listings, search results, analytics dashboards.
//
//  2. Session Consistency (Read-Your-Writes)
//     Within a session, reads always reflect the session's own writes.
//     Implemented by routing reads to primary for 5s after a write.
//     Tradeoff: slightly higher latency; requires session state.
//     Use for: user profile updates, cart modifications.
//
//  3. Bounded Staleness
//     Reads may be stale, but by no more than T seconds.
//     Implemented by checking replica lag and falling back to primary.
//     Tradeoff: predictable staleness bound; may increase primary load.
//     Use for: inventory display (show stock within 30s accuracy).
//
//  4. Strong Consistency (Linearizability)
//     Every read reflects the most recent write. All reads go to primary.
//     Tradeoff: highest latency, lowest availability (CP in CAP theorem).
//     Use for: checkout, payment, order creation, stock deduction.
//
// CAP Theorem:
//
//	During a network partition, choose:
//	- CP: reject requests rather than return stale data (primary-only reads)
//	- AP: return possibly stale data (replica reads, secondaryPreferred)
package consistency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Level represents a consistency guarantee.
type Level int

const (
	Eventual         Level = iota // stale reads OK; highest availability
	Session                       // read-your-writes within a session
	BoundedStaleness              // stale by at most MaxStaleness
	Strong                        // linearizable; always hits primary
)

func (l Level) String() string {
	switch l {
	case Eventual:
		return "eventual"
	case Session:
		return "session"
	case BoundedStaleness:
		return "bounded-staleness"
	case Strong:
		return "strong"
	default:
		return "unknown"
	}
}

// ParseLevel parses a string consistency level from HTTP query params.
func ParseLevel(s string) (Level, error) {
	switch s {
	case "eventual", "":
		return Eventual, nil
	case "session":
		return Session, nil
	case "bounded-staleness", "bounded":
		return BoundedStaleness, nil
	case "strong":
		return Strong, nil
	default:
		return Eventual, fmt.Errorf("unknown consistency level: %s", s)
	}
}

// ─── Context propagation ──────────────────────────────────────────────────────

type ctxKey struct{}

// Request carries per-request consistency requirements through the call stack.
type Request struct {
	Level          Level
	MaxStaleness   time.Duration // for BoundedStaleness (default 30s)
	SessionToken   string        // for Session consistency
	WriteTimestamp time.Time     // time of last write in this session
}

// WithConsistency attaches a consistency request to a context.
func WithConsistency(ctx context.Context, req Request) context.Context {
	return context.WithValue(ctx, ctxKey{}, req)
}

// FromContext extracts the consistency request. Returns Eventual if not set.
func FromContext(ctx context.Context) Request {
	if req, ok := ctx.Value(ctxKey{}).(Request); ok {
		return req
	}
	return Request{Level: Eventual}
}

// ─── Read routing ─────────────────────────────────────────────────────────────

// ReadTarget indicates which DB node to read from.
type ReadTarget int

const (
	ReadFromReplica ReadTarget = iota
	ReadFromPrimary
)

// ReplicaLagFunc returns the current replication lag in seconds for a replica.
type ReplicaLagFunc func(shardID, replicaIdx int) float64

// RouteRead decides whether to read from a replica or primary.
//
// Decision table:
//
//	Strong            → primary (always)
//	Session           → primary if session wrote within 5s, else replica
//	BoundedStaleness  → replica if lag < MaxStaleness, else primary
//	Eventual          → replica (always)
func RouteRead(ctx context.Context, shardID int, getLag ReplicaLagFunc) ReadTarget {
	req := FromContext(ctx)
	switch req.Level {
	case Strong:
		return ReadFromPrimary

	case Session:
		if !req.WriteTimestamp.IsZero() && time.Since(req.WriteTimestamp) < 5*time.Second {
			return ReadFromPrimary // read-your-writes: stay on primary for 5s after write
		}
		return ReadFromReplica

	case BoundedStaleness:
		maxStaleness := req.MaxStaleness
		if maxStaleness == 0 {
			maxStaleness = 30 * time.Second
		}
		if getLag != nil {
			lag := getLag(shardID, 0)
			if lag > maxStaleness.Seconds() {
				return ReadFromPrimary // replica too stale
			}
		}
		return ReadFromReplica

	default: // Eventual
		return ReadFromReplica
	}
}

// ─── Write concern ────────────────────────────────────────────────────────────

// WriteConcern specifies durability requirements for a write.
type WriteConcern struct {
	MinNodes int // min nodes that must ack (0 = majority)
	Timeout  time.Duration
	Sync     bool // wait for fsync (durable write)
}

// WriteConcernFor returns the appropriate write concern for a consistency level.
func WriteConcernFor(level Level) WriteConcern {
	switch level {
	case Strong:
		return WriteConcern{MinNodes: 0, Timeout: 5 * time.Second, Sync: true}
	case Session, BoundedStaleness:
		return WriteConcern{MinNodes: 1, Timeout: 3 * time.Second, Sync: false}
	default:
		return WriteConcern{MinNodes: 1, Timeout: 1 * time.Second, Sync: false}
	}
}

// ─── Session store ────────────────────────────────────────────────────────────

// SessionStore tracks per-session write timestamps for Session consistency.
// In production, store this in Redis with a TTL.
type SessionStore struct {
	mu     sync.RWMutex
	writes map[string]time.Time
}

var globalSessionStore = &SessionStore{writes: make(map[string]time.Time)}

// RecordWrite records that a write occurred in this session.
func RecordWrite(sessionToken string) {
	globalSessionStore.mu.Lock()
	globalSessionStore.writes[sessionToken] = time.Now()
	globalSessionStore.mu.Unlock()
}

// LastWrite returns the time of the last write for a session.
func LastWrite(sessionToken string) time.Time {
	globalSessionStore.mu.RLock()
	t := globalSessionStore.writes[sessionToken]
	globalSessionStore.mu.RUnlock()
	return t
}

// ─── Consistency validator ────────────────────────────────────────────────────

// ValidateRead checks if the data age satisfies the consistency requirement.
// dataAge is how old the data is (time since last write to the source).
func ValidateRead(ctx context.Context, dataAge time.Duration) error {
	req := FromContext(ctx)
	switch req.Level {
	case Strong:
		if dataAge > 100*time.Millisecond {
			return fmt.Errorf("%w: data age %v exceeds 100ms for strong consistency",
				ErrConsistencyViolation, dataAge)
		}
	case BoundedStaleness:
		maxStaleness := req.MaxStaleness
		if maxStaleness == 0 {
			maxStaleness = 30 * time.Second
		}
		if dataAge > maxStaleness {
			return fmt.Errorf("%w: data age %v exceeds bound %v",
				ErrReplicaTooStale, dataAge, maxStaleness)
		}
	case Session:
		if !req.WriteTimestamp.IsZero() && dataAge > time.Since(req.WriteTimestamp)+100*time.Millisecond {
			return fmt.Errorf("%w: stale read after session write", ErrConsistencyViolation)
		}
	}
	return nil
}

// ─── Response headers ─────────────────────────────────────────────────────────

// ResponseHeaders returns HTTP headers that communicate consistency guarantees
// to clients. Clients can use these to decide whether to retry with stronger
// consistency if they receive stale data.
func ResponseHeaders(level Level, dataAge time.Duration, fromReplica bool) map[string]string {
	h := map[string]string{
		"X-Consistency-Level": level.String(),
		"X-Data-Source":       "primary",
	}
	if fromReplica {
		h["X-Data-Source"] = "replica"
		h["X-Data-Age-Ms"] = fmt.Sprintf("%d", dataAge.Milliseconds())
	}
	return h
}

// ─── Errors ───────────────────────────────────────────────────────────────────

var (
	ErrConsistencyViolation = errors.New("consistency requirement not satisfied")
	ErrReplicaTooStale      = errors.New("replica lag exceeds staleness bound")
	ErrPrimaryRequired      = errors.New("strong consistency requires primary read")
)
