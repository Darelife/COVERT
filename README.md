# covert

A proof of concept for live, P2P, multi-user collaborative editing of a
directory, synced via a CRDT, with conflicts resolved by majority vote
(falling back to join-order priority), and the converged result streamed
into a live [jj](https://jj-vcs.dev) (git-backed) commit history.

No central server. Every peer runs the same `covert` binary and connects
directly to every other peer.

## How it works

COVERT resolves conflicts with one rule, applied at two levels: **vote, then
priority**. Contested content — a line's text, or a file's very existence —
is a small register of per-peer proposals. Whichever proposal has a strict
majority wins; on a tie (or no majority), the proposal from the peer with the
best join-priority wins. See [`pkg/crdt`](pkg/crdt/README.md) for the full
rule, including how file creation, deletion, and renaming reuse it.

Networking is a simple full mesh over plain TCP — every peer dials every
other peer directly, with gossip keeping the mesh self-healing. See
[`pkg/network`](pkg/network/README.md).

The working directory is a colocated git+jj repo. Every settled sync round
becomes a `jj commit`, debounced so rapid edits don't produce a commit per
keystroke. See [`pkg/jjrepo`](pkg/jjrepo/README.md).

Join-order priority (founder = 1, rejoin = demoted) is looked up live, so an
in-flight conflict re-resolves the instant a peer's priority changes. See
[`pkg/priority`](pkg/priority/README.md).

## Package layout

Each package's README explains only what's inside that package and how it
connects to its siblings — follow the links for depth.

```
pkg/crdt      the CRDT itself, incl. file/path-level create/delete/rename
pkg/priority  join-order priority table
pkg/network   full-mesh P2P transport
pkg/watch     filesystem watcher -> debounced callbacks
pkg/jjrepo    thin exec wrapper around the jj CLI
pkg/session   wires the above into one daemon
cmd/covert    CLI (init / join)
```

- [`pkg/crdt`](pkg/crdt/README.md)
- [`pkg/priority`](pkg/priority/README.md)
- [`pkg/network`](pkg/network/README.md)
- [`pkg/watch`](pkg/watch/README.md)
- [`pkg/jjrepo`](pkg/jjrepo/README.md)
- [`pkg/session`](pkg/session/README.md)
- [`cmd/covert`](cmd/covert/README.md)

## Testing

```sh
go test ./...
```

`pkg/crdt` has the load-bearing tests: commutative concurrent inserts,
majority vote, priority-chain tiebreak, rejoin demotion, and same-line
concurrent edits correctly contending as proposals (not silently becoming
two separate lines).
