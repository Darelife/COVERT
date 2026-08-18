# covert

A proof of concept for live, P2P, multi-user collaborative editing of a
directory, synced via a CRDT, with conflicts resolved by majority vote
(falling back to join-order priority), and the converged result streamed
into a live [jj](https://jj-vcs.dev) (git-backed) commit history.

No central server. Every peer runs the same `covert` binary and connects
directly to every other peer.

## Quick start

Requires Go and `jj` on your PATH.

```sh
go build -o covert ./cmd/covert
```

Start a session over a directory (you become priority 1, the founder):

```sh
./covert init ~/some-project
# covert session started
#   peer id: 3219fe94 (priority 1, founder)
#   listen:  127.0.0.1:54321
#   ...
# Others can join with:
#   covert join 127.0.0.1:54321
```

On another machine (or another terminal, another directory):

```sh
./covert join 127.0.0.1:54321 ~/some-project-copy
```

Edit files with any editor in either directory. Changes propagate live to
every peer, and every settled sync round becomes a `jj commit` in each
peer's local repo (`jj log` to see it happen).

Flags must come before positional arguments (standard Go `flag` parsing):
`covert join --listen 127.0.0.1:9000 <peer-addr> [dir]`.

## How it works

### Conflict resolution: vote, then priority

Every line in a file is a CRDT element with a permanent structural identity
(a fractional-index position + creator's peer ID, so **structural inserts
are always commutative and never conflict** — two peers inserting near the
same spot just both survive, ordered deterministically).

A line's *content*, however, is a small multi-value register: each peer that
edits a line submits a **proposal**, keyed by their peer ID. When materializing
the document, each contested line is resolved by:

1. **Majority vote** — if one proposed value has strictly more than half the
   votes among peers who proposed *something* for that line, it wins outright.
2. **Priority chain** — on a tie, or when nothing clears 50%, the proposal
   from whichever peer currently holds the best (numerically lowest)
   join-priority wins.

Priority is assigned by join order: the founder is 1, the next joiner is 2,
and so on. **Rejoining always gets a fresh, worse priority number** — leave
and come back, and you're now the lowest priority, exactly as if you were the
newest joiner. Because priority is looked up live (not baked into old
proposals), this takes effect immediately: an in-flight conflict re-resolves
the instant a peer's priority changes.

Peer identity is persisted per working directory (`.covert/identity`), so a
process restart in the same directory resumes as *the same peer* — its
priority is genuinely demoted, rather than orphaning a stale identity while a
disconnected stranger identity starts fresh.

### Networking

A simple full mesh over plain TCP: every peer dials every other peer
directly (no relay, no DHT). Joining connects to one known member, which
assigns you a priority and hands you its peer list; you then dial everyone
on that list yourself, and gossip keeps the mesh self-healing as new peers
show up. On every new connection (join or mesh-repair), both sides replicate
their full CRDT state to each other, so joining mid-session gets you full
history, not just future edits.

**Known POC limitation:** priority numbers are assigned locally by whichever
peer you connect through, with no global consensus — two peers joining
through different members at the exact same instant could race for the same
number. Ties are broken deterministically (by peer ID) so this never corrupts
state, just occasionally picks an arbitrary-but-consistent order between
simultaneous joiners.

### Live jj commits

The working directory is a colocated git+jj repo (`jj git init`). Every time
a sync round changes any file's resolved content, `covert` writes the
resolved content to disk and runs `jj commit -m "sync: <files> (by <peers>)"`
— jj's own working-copy auto-snapshot picks up the change, so this both
finalizes the description and opens a fresh commit on top. Multiple rapid
edits are debounced into one commit rather than one per keystroke.

## Package layout

```
pkg/crdt      the CRDT itself: fractional-index line IDs, per-line proposal
              voting/priority resolution, and the line-diff -> ops reconciler
pkg/priority  join-order priority table (founder=1, rejoin=demoted)
pkg/network   full-mesh P2P transport (TCP, JSON messages)
pkg/watch     filesystem watcher -> debounced whole-file-content callbacks
pkg/jjrepo    thin exec wrapper around the jj CLI
pkg/session   wires the above into one daemon over a working directory
cmd/covert    CLI (init / join)
```

## Testing

```sh
go test ./...
```

`pkg/crdt` has the load-bearing tests: commutative concurrent inserts,
majority vote, priority-chain tiebreak, rejoin demotion, and same-line
concurrent edits correctly contending as proposals (not silently becoming
two separate lines).
