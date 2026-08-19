# pkg/crdt

The CRDT: how COVERT decides what a file's lines say and which files exist,
when multiple peers disagree.

## Line identity: fractional index + creator peer ID

Every line has a permanent structural identity: a fractional-index position
plus its creator's peer ID. This makes **structural inserts always
commutative** — two peers inserting near the same spot just both survive,
ordered deterministically by their fractional index. Identity never
collides, because the creator's peer ID is part of it.

## Content registers: vote, then priority

A line's *content* is a separate concern from its identity — it's a small
register of **proposals**, one slot per peer, keyed by peer ID. Materializing
the document resolves each contested register by:

1. **Majority vote** — if one proposed value has strictly more than half the
   votes among peers who proposed *something* for that register, it wins
   outright.
2. **Priority chain** — on a tie, or when nothing clears 50%, the proposal
   from whichever peer currently holds the best (numerically lowest)
   join-priority wins (see [`pkg/priority`](../priority/README.md)).

Priority is looked up live, not baked into the proposal — a priority change
re-resolves any in-flight contested register immediately.

## File/path-level ops reuse the same register

A file's path is an identity exactly like a line's fractional index, and its
existence-and-content is a register exactly like a line's content register.
This means create/delete/rename need no new resolution mechanism — they're
the same vote-then-priority rule, just keyed by path instead of line ID.

- **Create**: uncontested in the common case (one peer creates a path, it
  propagates). The only real conflict is two peers creating the *same* path
  with different content before either has seen the other's create — that's
  structurally identical to two peers editing the same existing line
  concurrently, and resolves the same way.
- **Delete vs. concurrent edit**: edit wins, but *only* if the edit is
  genuinely concurrent with the delete. A delete proposal that merely lost to
  some peer's stale, already-resolved edit from an earlier round must still
  succeed — otherwise any file anyone ever touched could never be deleted.
  Concurrency is tracked with a **per-file version counter**: it increments
  each time the file's register changes within the current, still-open sync
  round, and **resets to zero once that round is written as a jj commit**
  (see [`pkg/jjrepo`](../jjrepo/README.md) and
  [`pkg/session`](../session/README.md), which triggers the reset). A delete
  proposal only loses to edits with a version newer than what the delete
  itself had already observed. Because the counter resets on every commit, it
  only ever needs to span "since the last commit" — it never grows unbounded.
- **Rename**: not handled here at all. It arrives at this package already
  decomposed into a delete-old-path + create-new-path pair (see
  [`pkg/watch`](../watch/README.md)), so it's just two ordinary register
  operations. A rename of a file that's concurrently being edited elsewhere
  behaves like any other edit-beats-delete case: both the old path (with the
  edit) and the new path end up existing.

## Implementation

### Core types

```go
type PeerID string

// FracIndex is a Logoot/LSEQ-style fractional index: a sequence of digits
// compared lexicographically. Insertion always allocates strictly between
// its two neighbors, so no two structural positions ever collide.
type FracIndex []uint32

type LineID struct {
    Pos  FracIndex
    Peer PeerID // creator; only used to break a same-Pos tie, which
                 // GenerateBetween (below) is designed to never produce
}

// Proposal is one peer's claim about a register's value.
type Proposal struct {
    Peer      PeerID
    Value     string // line content; ignored when Tombstone is true
    Tombstone bool   // true = this peer proposes "line/path does not exist"
    Version   uint64 // File.Version at the moment this proposal was made
}

// Register is a contested slot: at most one live proposal per peer.
type Register struct {
    Proposals map[PeerID]Proposal
}

type Line struct {
    ID  LineID
    Reg Register
}

type File struct {
    Path    string
    Reg     Register         // existence register, keyed by path (not by LineID)
    Lines   map[LineID]*Line
    Order   []LineID         // cache, sorted by Pos; rebuilt on structural change
    Version uint64           // bumped on every register mutation in this file,
                              // reset to 0 when pkg/jjrepo confirms a commit
    deleteObservedAt uint64  // Version a pending delete proposal last saw
}

type Document struct {
    mu    sync.RWMutex
    Files map[string]*File
}

// PriorityLookup decouples this package from pkg/priority's concrete type.
type PriorityLookup interface {
    Lookup(PeerID) int // lower wins; unknown peers return math.MaxInt
}
```

### Resolving a register: vote, then priority

```go
func (r *Register) Resolve(prio PriorityLookup) Proposal {
    counts := map[string]int{}
    for _, p := range r.Proposals {
        counts[proposalKey(p)]++ // Tombstone proposals key on a sentinel, not Value
    }
    total := len(r.Proposals)
    for key, n := range counts {
        if n*2 > total {
            return firstProposalWithKey(r, key) // strict majority: done
        }
    }
    var best Proposal
    bestPrio := math.MaxInt
    for _, p := range r.Proposals {
        if pr := prio.Lookup(p.Peer); pr < bestPrio {
            bestPrio, best = pr, p
        }
    }
    return best
}
```

### Fractional index allocation

`GenerateBetween(a, b FracIndex, peer PeerID) FracIndex` walks `a` and `b`
digit by digit and picks a value strictly between the first pair that
differs (treating a missing digit as 0). If `a` and `b` are adjacent (no gap
exists at any depth), it appends one extra digit derived from `peer`'s ID
before recursing one level deeper. Two peers concurrently inserting "at the
same spot" call this independently against the same neighbor pair and get
different `FracIndex` values because their appended tiebreak digit differs —
this is what makes structural inserts commutative without coordination.

### Delete-vs-edit: the version check

```go
func (f *File) ProposeDelete(peer PeerID, observedVersion uint64) {
    f.Reg.Proposals[peer] = Proposal{Peer: peer, Tombstone: true, Version: f.Version}
    f.deleteObservedAt = observedVersion
    f.Version++
}

func (f *File) ResolveExistence(prio PriorityLookup) Proposal {
    winner := f.Reg.Resolve(prio)
    if winner.Tombstone {
        for _, p := range f.Reg.Proposals {
            if !p.Tombstone && p.Version > f.deleteObservedAt {
                return p // an edit strictly newer than what the delete saw wins
            }
        }
    }
    return winner
}
```

`Document.ResetVersion(path)` zeroes `File.Version` (and clears resolved
tombstones/stale proposals) once [`pkg/session`](../session/README.md)
confirms [`pkg/jjrepo`](../jjrepo/README.md) has committed the round —
that's the only place this method is called from.

## Open questions

Not yet decided — flagged here rather than implied as settled:

- **Directories**: mkdir/rmdir semantics, and whether deleting a directory
  while a file inside it is concurrently edited follows the same
  delete-vs-edit rule or needs different (cascading) logic.
- **Binary files**: no line-level content to diff. Likely candidate is
  "whole file as a single register value" (reusing the same vote-then-priority
  mechanism at file granularity instead of per-line), but unconfirmed.
