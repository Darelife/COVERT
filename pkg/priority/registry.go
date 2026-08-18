// Package priority tracks each peer's join-order priority: the founder is 1,
// each new joiner gets the next integer, and any peer that (re)joins always
// receives a brand new (worse) number, pushing it to the back of the line.
package priority

import "sync"

type Registry struct {
	mu    sync.Mutex
	next  int
	table map[string]int // peerID -> priority (lower = higher priority)
}

func NewRegistry() *Registry {
	return &Registry{next: 1, table: make(map[string]int)}
}

// InitFounder must be called exactly once, by the peer starting a brand new
// session, before anyone else joins. It claims priority 1.
func (r *Registry) InitFounder(peerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.table[peerID] = 1
	if r.next < 2 {
		r.next = 2
	}
}

// Assign hands out the next priority number to peerID, unconditionally
// overwriting any priority it may have held before (a rejoin demotion).
func (r *Registry) Assign(peerID string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.next
	r.next++
	r.table[peerID] = p
	return p
}

// Observe records a priority learned from gossip/another peer. It only moves
// r.next forward so future local Assign calls never hand out a number that's
// already taken.
func (r *Registry) Observe(peerID string, p int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.table[peerID] = p
	if p >= r.next {
		r.next = p + 1
	}
}

func (r *Registry) Get(peerID string) (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.table[peerID]
	return p, ok
}

// Snapshot returns a copy of the full peerID -> priority table, suitable for
// passing straight into crdt.Document.Materialize.
func (r *Registry) Snapshot() map[string]int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]int, len(r.table))
	for k, v := range r.table {
		out[k] = v
	}
	return out
}
