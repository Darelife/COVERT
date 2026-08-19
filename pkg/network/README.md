# pkg/network

The P2P transport: a full mesh over plain TCP. No central server, no relay,
no DHT — every peer dials every other peer directly.

## Join flow

Joining connects to one known member, which assigns you a priority (see
[`pkg/priority`](../priority/README.md)) and hands you its peer list. You
then dial everyone on that list yourself, and gossip keeps the mesh
self-healing as new peers show up or reconnect.

## Replication on connect

On every new connection — whether from a join or a mesh-repair reconnect —
both sides replicate their full CRDT state to each other (see
[`pkg/crdt`](../crdt/README.md)). Joining mid-session gets you full history,
not just future edits.

## Implementation

### Wire format

Every connection is a raw `net.Conn` (TCP) carrying length-prefixed frames:
a 4-byte big-endian `uint32` byte count, followed by a gob-encoded
`Envelope`. One reader goroutine per connection decodes frames into a
per-connection inbound channel; writes go through a per-connection mutex so
concurrent senders (the sync-round broadcaster and the gossip ticker) can't
interleave partial frames.

```go
type MsgType uint8

const (
    MsgHello MsgType = iota
    MsgPeerList
    MsgPriorityAssign
    MsgCRDTSnapshot // full pkg/crdt.Document, sent once per new connection
    MsgCRDTDelta    // incremental Register updates, sent per sync round
)

type Envelope struct {
    Type    MsgType
    Payload []byte // gob-encoded, concrete type keyed by Type
}

type HelloMsg struct{ Peer priority.PeerID }
type PeerListMsg struct{ Addrs map[priority.PeerID]string }
type PriorityAssignMsg struct {
    Peer priority.PeerID
    Number int
}
```

### Mesh state

```go
type Mesh struct {
    mu    sync.Mutex
    self  priority.PeerID
    conns map[priority.PeerID]net.Conn
    addrs map[priority.PeerID]string // for reconnect-on-drop
}

func (m *Mesh) Broadcast(env Envelope) // best-effort fan-out to all conns
func (m *Mesh) dial(peer priority.PeerID, addr string) // used by join + gossip repair
```

### Join flow (concrete exchange)

1. Joiner dials `peerAddr`, sends `HelloMsg{joinerID}`.
2. Contact peer calls `priority.Table.Assign(joinerID)`, replies with, in
   order: `PriorityAssignMsg{joinerID, n}`, `PeerListMsg{known addrs}`,
   `CRDTSnapshot{full Document}`.
3. Joiner applies the snapshot to its local `pkg/crdt.Document`, then dials
   every address in `PeerListMsg` (skipping itself), performing the same
   `Hello` → snapshot exchange with each — but those peers call `Table.set`
   (already have the number), not `Assign`.
4. The contact peer also broadcasts `PriorityAssignMsg{joinerID, n}` and an
   updated `PeerListMsg` to its existing connections, so the rest of the
   mesh dials the joiner without waiting for step 3 to reach them.

### Gossip / self-healing

Each peer runs a ticker (e.g. every 5s) that broadcasts its current
`PeerListMsg`. On receipt, a peer diffs incoming addrs against `Mesh.conns`
and calls `dial` on anything missing (new peer, or a dropped connection
whose `addr` is still remembered) with jittered exponential backoff per
target to avoid a reconnect storm after a network blip.

## Known POC limitation

Priority assignment during join has no global consensus (see
[`pkg/priority`](../priority/README.md#known-poc-limitation)) — this package
is where that race can occur, since it's whichever peer you connect through
that hands out the number.
