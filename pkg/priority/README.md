# pkg/priority

The join-order priority table used as the tiebreak step in
[`pkg/crdt`](../crdt/README.md)'s vote-then-priority resolution.

## Assignment

Priority is assigned by join order: the founder is 1, the next joiner is 2,
and so on. **Rejoining always gets a fresh, worse priority number** — leave
and come back, and you're now the lowest priority, exactly as if you were the
newest joiner.

Peer identity is persisted per working directory (`.covert/identity`), so a
process restart in the same directory resumes as *the same peer* — its
priority is genuinely demoted, rather than orphaning a stale identity while a
disconnected stranger identity starts fresh.

## Live lookup

Priority is looked up live at resolution time, never baked into a proposal.
This means an in-flight conflict in `pkg/crdt` re-resolves the instant a
peer's priority changes (e.g. right after a rejoin-demotion).

## Implementation

```go
type PeerID string

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
```

### Identity persistence

`.covert/identity` in the working directory holds this peer's own `PeerID`
(a ULID, generated once on first `init`/`join` and written with `O_EXCL` so
it's never silently regenerated). On process start, [`pkg/session`](../session/README.md)
reads this file if present, else generates and writes it — that's what
makes a restart resume as *the same* peer (and therefore reconnect at its
already-demoted priority rather than as a fresh unknown).

### Propagation

`Table` itself never touches the network. [`pkg/network`](../network/README.md)
calls `Assign` on the contact peer during a join and then gossips a
`MsgPriorityAssign{Peer, Number}` to the rest of the mesh, each of which
calls `t.set(Peer, Number)` locally to converge.

## Known POC limitation

Priority numbers are assigned locally by whichever peer you connect through
(see [`pkg/network`](../network/README.md)'s join flow), with no global
consensus — two peers joining through different members at the exact same
instant could race for the same number. Ties are broken deterministically
(by peer ID) so this never corrupts state, just occasionally picks an
arbitrary-but-consistent order between simultaneous joiners.
