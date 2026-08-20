// Package network is the P2P transport: a full mesh over plain TCP. No
// central server, no relay, no DHT — every peer dials every other peer
// directly, with gossip keeping the mesh self-healing.
package network

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"sync"

	"github.com/darelife/covert/pkg/crdt"
	"github.com/darelife/covert/pkg/priority"
)

type MsgType uint8

const (
	MsgHello MsgType = iota
	MsgPeerList
	MsgPriorityAssign
	MsgCRDTSnapshot // full pkg/crdt.Document, sent once per new connection
	MsgCRDTDelta    // incremental Register updates, sent per sync round
)

// Envelope is the wire-level frame: a message type plus its gob-encoded
// concrete payload.
type Envelope struct {
	Type    MsgType
	Payload []byte
}

// HelloMsg carries the dialer's own listen address (not derivable from the
// TCP connection itself, which only exposes the ephemeral source port) so
// the acceptor can register a dialable address for gossip-driven repair.
type HelloMsg struct {
	Peer priority.PeerID
	Addr string
}

// PeerListMsg carries both the mesh's known addresses and every currently
// assigned priority number — a learner needs both: addrs to complete the
// mesh, priorities so Register.Resolve's tiebreak doesn't treat an
// already-ranked peer as unknown (see priority.Table.Snapshot).
type PeerListMsg struct {
	Addrs      map[priority.PeerID]string
	Priorities map[priority.PeerID]int
}

type PriorityAssignMsg struct {
	Peer   priority.PeerID
	Number int
}

// CRDTFilesMsg carries a set of pkg/crdt.File values — the full set for a
// MsgCRDTSnapshot, or just the touched files for a MsgCRDTDelta. Both are
// merged into the receiver's Document the same way (crdt.Document.ApplyRemote
// is a proposal-wise union, safe to apply redundantly or out of order).
type CRDTFilesMsg struct {
	Files []*crdt.File
}

func encodeEnvelope(t MsgType, v any) (Envelope, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return Envelope{}, fmt.Errorf("encoding %T payload: %w", v, err)
	}
	return Envelope{Type: t, Payload: buf.Bytes()}, nil
}

func decodePayload(payload []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(payload)).Decode(v)
}

// writeFrame writes env as a 4-byte big-endian length prefix followed by
// its gob encoding. Callers must serialize writes to the same io.Writer
// themselves (see connWriter).
func writeFrame(w io.Writer, env Envelope) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(env); err != nil {
		return err
	}
	var lenPrefix [4]byte
	binary.BigEndian.PutUint32(lenPrefix[:], uint32(buf.Len()))
	if _, err := w.Write(lenPrefix[:]); err != nil {
		return err
	}
	_, err := w.Write(buf.Bytes())
	return err
}

// readFrame reads one length-prefixed, gob-encoded Envelope.
func readFrame(r io.Reader) (Envelope, error) {
	var lenPrefix [4]byte
	if _, err := io.ReadFull(r, lenPrefix[:]); err != nil {
		return Envelope{}, err
	}
	n := binary.BigEndian.Uint32(lenPrefix[:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Envelope{}, err
	}
	var env Envelope
	if err := gob.NewDecoder(bytes.NewReader(buf)).Decode(&env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// connWriter serializes frame writes to one connection, so the sync-round
// broadcaster and the gossip ticker can't interleave partial frames.
type connWriter struct {
	mu   sync.Mutex
	conn io.Writer
}

func (c *connWriter) send(env Envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeFrame(c.conn, env)
}
