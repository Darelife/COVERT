# pkg/session

Wires everything else into one daemon over a working directory:
[`pkg/watch`](../watch/README.md) → [`pkg/crdt`](../crdt/README.md) →
[`pkg/network`](../network/README.md) → [`pkg/jjrepo`](../jjrepo/README.md).

## Sync-round boundary

This package owns the debounce/sync-round boundary. `pkg/crdt`'s per-file
version counters are scoped to whatever this package considers "the current
round" — they increment as changes land within it, and this package tells
`pkg/crdt` to reset them once `pkg/jjrepo` confirms the round has been
committed.

## Responsibilities

- Feed `pkg/watch` callbacks into `pkg/crdt` as proposals.
- Feed `pkg/crdt`'s resolved changes out over `pkg/network` to other peers,
  and incoming peer proposals from `pkg/network` into `pkg/crdt`.
- After a round settles, hand the resolved content to `pkg/jjrepo` for
  committing, then trigger the version-counter reset in `pkg/crdt`.

## Implementation

```go
type Session struct {
    dir     string
    self    priority.PeerID
    doc     *crdt.Document
    prio    *priority.Table
    mesh    *network.Mesh
    repo    *jjrepo.Repo
    watcher *watch.Watcher

    roundMu     sync.Mutex
    dirty       map[string]bool // touched since the last commit
    commitTimer *time.Timer
    commitDelay time.Duration // e.g. 500ms; deliberately separate from
                                // pkg/watch's own debounce (that one coalesces
                                // OS-level writes into a FileEvent; this one
                                // coalesces FileEvents/network deltas into a commit)
}

func (s *Session) Run(ctx context.Context) error {
    events := s.watcher.Events()
    inbound := s.mesh.Incoming()
    for {
        select {
        case ev := <-events:
            s.applyLocalEvent(ev)
        case env := <-inbound:
            s.applyRemote(env)
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}
```

### Local edit → proposals

`applyLocalEvent` diffs the file's previous known content against the new
`FileEvent.Content` line-by-line (Myers/LCS), then per line:

- **unchanged** — no register touched.
- **inserted** — new `crdt.LineID` via `crdt.GenerateBetween` on its
  neighbors, a `Line` with a single self-proposal.
- **deleted** — `ProposeDelete` on the existing `Line`'s register.
- **changed** — new self-`Proposal` written into the existing `Line`'s
  register, `File.Version++`.

A whole-file create/delete is the same shape one level up, on `File.Reg`
instead of a `Line.Reg` (see [`pkg/crdt`](../crdt/README.md#file-path-level-ops-reuse-the-same-register)).
Every touched file is added to `s.dirty`, the result is broadcast as a
`MsgCRDTDelta`, and `armCommitTimer` (re)starts the debounce below.

### Remote deltas / snapshots

`applyRemote` merges an incoming `MsgCRDTDelta` or `MsgCRDTSnapshot` into
`s.doc` (proposal-wise union per register — never a blind overwrite, so a
delta that arrives out of order still just adds its proposals), marks
affected files dirty, and arms the same commit timer.

### Commit debounce

```go
func (s *Session) armCommitTimer() {
    s.roundMu.Lock()
    defer s.roundMu.Unlock()
    if s.commitTimer != nil {
        s.commitTimer.Stop()
    }
    s.commitTimer = time.AfterFunc(s.commitDelay, s.settleRound)
}

func (s *Session) settleRound() {
    s.roundMu.Lock()
    files := make([]string, 0, len(s.dirty))
    for p := range s.dirty {
        files = append(files, p)
    }
    s.dirty = map[string]bool{}
    s.roundMu.Unlock()

    changes := make([]jjrepo.Change, 0, len(files))
    for _, p := range files {
        f := s.doc.Files[p]
        resolved := f.ResolveExistence(s.prio)
        changes = append(changes, jjrepo.Change{
            Path: p, Content: []byte(resolved.Value), Deleted: resolved.Tombstone,
        })
    }
    if err := s.repo.Commit(changes, contributingPeers(s.doc, files)); err != nil {
        log.Printf("commit failed, will retry next round: %v", err)
        return // files stay out of s.dirty; next incoming event re-dirties them
    }
    for _, p := range files {
        s.doc.ResetVersion(p)
    }
}
```

A failed commit is not retried on a timer of its own — it relies on the
fact that any further local or remote change to the same file re-arms the
debounce and re-attempts. A file that's genuinely quiesced and still failed
to commit stays uncommitted until touched again; acceptable for a POC.
