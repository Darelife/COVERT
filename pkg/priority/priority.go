// Package priority implements the join-order priority table used as the
// tiebreak step in pkg/crdt's vote-then-priority conflict resolution.
package priority

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/oklog/ulid/v2"
)

type PeerID string

// Table is the live join-order priority table. Lower number wins ties.
type Table struct {
	mu   sync.RWMutex
	prio map[PeerID]int
	next int // next number to hand out; starts at 2 (founder claims 1 directly)
}

func New() *Table { return &Table{prio: map[PeerID]int{}, next: 2} }

// AssignFounder is called exactly once, by cmd/covert's `init` path.
func (t *Table) AssignFounder(self PeerID) { t.set(self, 1) }

// Assign hands out the next-worst number. Called by pkg/network on the
// contact peer's side of a join (fresh join) and again on every rejoin —
// there is no special-case "already known" path, so a rejoin always lands
// a strictly worse number than its previous one.
func (t *Table) Assign(p PeerID) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := t.next
	t.next++
	t.prio[p] = n
	return n
}

// Set records a priority number learned from elsewhere (e.g. a
// PriorityAssignMsg received over the mesh). Exported so pkg/network can
// converge peers that didn't do the assigning themselves.
func (t *Table) Set(p PeerID, n int) { t.set(p, n) }

func (t *Table) set(p PeerID, n int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prio[p] = n
	if n >= t.next {
		t.next = n + 1
	}
}

// Lookup implements pkg/crdt's PriorityLookup interface. Unknown peers
// (not yet observed) sort last, never winning a tie.
func (t *Table) Lookup(p PeerID) int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if n, ok := t.prio[p]; ok {
		return n
	}
	return math.MaxInt
}

// Snapshot returns a copy of every peer number currently known. pkg/network
// includes this in PeerListMsg so a joiner (or a gossip-repair reconnect)
// learns every already-assigned peer's number, not just its own — without
// it, an unknown peer would sort last in Register.Resolve's priority
// tiebreak on the learner's side even though it's genuinely well-ranked.
func (t *Table) Snapshot() map[PeerID]int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[PeerID]int, len(t.prio))
	for p, n := range t.prio {
		out[p] = n
	}
	return out
}

const identityFile = ".covert/identity"

// LoadOrCreateIdentity returns this peer's persistent PeerID for the given
// working directory, generating and persisting a new ULID on first use.
// Because the file is written with O_EXCL, a process restart in the same
// directory resumes as the same peer instead of silently regenerating one.
func LoadOrCreateIdentity(dir string) (PeerID, error) {
	path := filepath.Join(dir, identityFile)

	if b, err := os.ReadFile(path); err == nil {
		return PeerID(b), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}

	id := PeerID(ulid.Make().String())
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			// Lost a race with another process; read back the winner.
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return "", rerr
			}
			return PeerID(b), nil
		}
		return "", fmt.Errorf("creating identity file: %w", err)
	}
	defer f.Close()
	if _, err := f.WriteString(string(id)); err != nil {
		return "", err
	}
	return id, nil
}
