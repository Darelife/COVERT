package network

import (
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/darelife/covert/pkg/crdt"
	"github.com/darelife/covert/pkg/priority"
)

// Delta is a decoded, ready-to-merge batch of remote file state, handed to
// pkg/session over Incoming().
type Delta struct {
	Files []*crdt.File
}

const defaultGossipInterval = 3 * time.Second

// Mesh is one peer's view of the full-mesh network: its own listener, and
// a direct connection to every other peer it knows about.
type Mesh struct {
	self priority.PeerID
	prio *priority.Table
	doc  *crdt.Document

	ln net.Listener

	mu    sync.Mutex
	conns map[priority.PeerID]*connWriter
	addrs map[priority.PeerID]string

	incoming chan Delta
	done     chan struct{}
	closeOne sync.Once

	gossipInterval time.Duration
}

func newMesh(self priority.PeerID, prio *priority.Table, doc *crdt.Document, ln net.Listener) *Mesh {
	m := &Mesh{
		self:           self,
		prio:           prio,
		doc:            doc,
		ln:             ln,
		conns:          map[priority.PeerID]*connWriter{},
		addrs:          map[priority.PeerID]string{},
		incoming:       make(chan Delta, 64),
		done:           make(chan struct{}),
		gossipInterval: defaultGossipInterval,
	}
	go m.acceptLoop()
	go m.gossipLoop()
	return m
}

// NewFounder binds listenAddr, assigns self priority 1 (the founder), and
// starts accepting connections. No join handshake is performed.
func NewFounder(self priority.PeerID, prio *priority.Table, doc *crdt.Document, listenAddr string) (*Mesh, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	prio.AssignFounder(self)
	return newMesh(self, prio, doc, ln), nil
}

// NewJoiner binds listenAddr, then joins the mesh through contactAddr: Hello
// -> PriorityAssign/PeerList/Snapshot from the contact, then dialing every
// other peer in that list to complete the mesh.
func NewJoiner(self priority.PeerID, prio *priority.Table, doc *crdt.Document, listenAddr, contactAddr string) (*Mesh, error) {
	ln, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	m := newMesh(self, prio, doc, ln)

	if err := m.join(contactAddr); err != nil {
		m.Close()
		return nil, err
	}
	return m, nil
}

func (m *Mesh) ListenAddr() string { return m.ln.Addr().String() }

// join performs the joiner's side of the exchange documented in this
// package's README: dial the contact, learn our assigned priority number
// and the mesh's peer list, apply the contact's CRDT snapshot, then dial
// every other known peer to complete the mesh.
func (m *Mesh) join(contactAddr string) error {
	conn, err := net.Dial("tcp", contactAddr)
	if err != nil {
		return fmt.Errorf("dialing %s: %w", contactAddr, err)
	}

	if err := writeFrame(conn, mustEnvelope(MsgHello, HelloMsg{Peer: m.self, Addr: m.ListenAddr()})); err != nil {
		conn.Close()
		return err
	}

	var assign PriorityAssignMsg
	var peerList PeerListMsg
	var snapshot CRDTFilesMsg
	for _, dst := range []struct {
		typ MsgType
		v   any
	}{
		{MsgPriorityAssign, &assign},
		{MsgPeerList, &peerList},
		{MsgCRDTSnapshot, &snapshot},
	} {
		env, err := readFrame(conn)
		if err != nil {
			conn.Close()
			return fmt.Errorf("reading join reply: %w", err)
		}
		if env.Type != dst.typ {
			conn.Close()
			return fmt.Errorf("join handshake: expected msg type %d, got %d", dst.typ, env.Type)
		}
		if err := decodePayload(env.Payload, dst.v); err != nil {
			conn.Close()
			return err
		}
	}

	m.prio.Set(m.self, assign.Number)
	for peer, n := range peerList.Priorities {
		m.prio.Set(peer, n)
	}
	m.deliver(snapshot.Files)

	// Register the contact connection under whichever peer ID in the
	// returned peer list maps to the address we just dialed.
	contactPeer := priority.PeerID("")
	for peer, addr := range peerList.Addrs {
		if addr == contactAddr {
			contactPeer = peer
			break
		}
	}
	m.registerConn(contactPeer, contactAddr, conn)
	go m.readLoop(contactPeer, conn)

	// Dial every other known peer to complete the mesh.
	for peer, addr := range peerList.Addrs {
		if peer == m.self || peer == contactPeer || addr == "" {
			continue
		}
		go m.dialPeer(peer, addr)
	}

	return nil
}

// dialPeer opens an outbound connection to an already-known peer (mesh
// completion during join, or gossip-driven reconnect) and performs the
// same Hello/reply exchange as join, but the far side will resolve our ID
// via priority.Table.Set rather than Assign, since it already knows us.
func (m *Mesh) dialPeer(peer priority.PeerID, addr string) {
	m.mu.Lock()
	_, already := m.conns[peer]
	m.mu.Unlock()
	if already {
		return
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return // best-effort; gossip will retry
	}

	if err := writeFrame(conn, mustEnvelope(MsgHello, HelloMsg{Peer: m.self, Addr: m.ListenAddr()})); err != nil {
		conn.Close()
		return
	}

	for i := 0; i < 3; i++ {
		env, err := readFrame(conn)
		if err != nil {
			conn.Close()
			return
		}
		switch env.Type {
		case MsgPriorityAssign:
			var a PriorityAssignMsg
			if decodePayload(env.Payload, &a) == nil {
				m.prio.Set(a.Peer, a.Number)
			}
		case MsgPeerList:
			var pl PeerListMsg
			if decodePayload(env.Payload, &pl) == nil {
				m.reconcilePeerList(pl)
			}
		case MsgCRDTSnapshot:
			var snap CRDTFilesMsg
			if decodePayload(env.Payload, &snap) == nil {
				m.deliver(snap.Files)
			}
		}
	}

	m.registerConn(peer, addr, conn)
	go m.readLoop(peer, conn)
}

func (m *Mesh) registerConn(peer priority.PeerID, addr string, conn net.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[peer] = &connWriter{conn: conn}
	if addr != "" {
		m.addrs[peer] = addr
	}
}

func (m *Mesh) acceptLoop() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			select {
			case <-m.done:
				return
			default:
				return
			}
		}
		go m.handleInbound(conn)
	}
}

// handleInbound is the acceptor's side of the join exchange: read Hello,
// assign (or confirm) a priority number, reply with that number, the
// current peer list, and a full CRDT snapshot — then, if this was a
// genuinely new peer, gossip its arrival to everyone already connected.
func (m *Mesh) handleInbound(conn net.Conn) {
	env, err := readFrame(conn)
	if err != nil || env.Type != MsgHello {
		conn.Close()
		return
	}
	var hello HelloMsg
	if err := decodePayload(env.Payload, &hello); err != nil {
		conn.Close()
		return
	}
	peer := hello.Peer

	isNew := m.prio.Lookup(peer) == math.MaxInt
	var n int
	if isNew {
		n = m.prio.Assign(peer)
	} else {
		n = m.prio.Lookup(peer)
	}

	cw := &connWriter{conn: conn}
	m.mu.Lock()
	m.conns[peer] = cw
	if hello.Addr != "" {
		m.addrs[peer] = hello.Addr
	}
	addrsSnapshot := m.snapshotAddrsLocked()
	m.mu.Unlock()

	send := func(t MsgType, v any) bool {
		e, err := encodeEnvelope(t, v)
		if err != nil {
			return false
		}
		return cw.send(e) == nil
	}

	if !send(MsgPriorityAssign, PriorityAssignMsg{Peer: peer, Number: n}) {
		return
	}
	if !send(MsgPeerList, PeerListMsg{Addrs: addrsSnapshot, Priorities: m.prio.Snapshot()}) {
		return
	}
	if !send(MsgCRDTSnapshot, CRDTFilesMsg{Files: m.allFiles()}) {
		return
	}

	if isNew {
		m.broadcastExcept(peer, mustEnvelope(MsgPriorityAssign, PriorityAssignMsg{Peer: peer, Number: n}))
		m.broadcastExcept(peer, mustEnvelope(MsgPeerList, m.peerListMsg()))
	}

	m.readLoop(peer, conn)
}

func (m *Mesh) readLoop(peer priority.PeerID, conn net.Conn) {
	defer func() {
		m.mu.Lock()
		if m.conns[peer] != nil && m.conns[peer].conn == conn {
			delete(m.conns, peer)
		}
		m.mu.Unlock()
		conn.Close()
	}()

	for {
		env, err := readFrame(conn)
		if err != nil {
			return
		}
		switch env.Type {
		case MsgCRDTDelta, MsgCRDTSnapshot:
			var msg CRDTFilesMsg
			if err := decodePayload(env.Payload, &msg); err != nil {
				continue
			}
			m.deliver(msg.Files)
		case MsgPeerList:
			var pl PeerListMsg
			if decodePayload(env.Payload, &pl) == nil {
				m.reconcilePeerList(pl)
			}
		case MsgPriorityAssign:
			var a PriorityAssignMsg
			if decodePayload(env.Payload, &a) == nil {
				m.prio.Set(a.Peer, a.Number)
			}
		}
	}
}

// deliver hands a decoded batch of remote files to pkg/session over
// Incoming(), WITHOUT merging them into m.doc here: pkg/session owns doc
// mutation from its single-threaded Run loop, so a merge on this goroutine
// (network's read/join/dial paths all run on their own goroutines) could
// race a concurrent local edit touching the same crdt.File. See this
// package's README section on Incoming() / pkg/session's applyRemote.
func (m *Mesh) deliver(files []*crdt.File) {
	if len(files) == 0 {
		return
	}
	select {
	case m.incoming <- Delta{Files: files}:
	case <-m.done:
	}
}

func (m *Mesh) allFiles() []*crdt.File {
	paths := m.doc.Paths()
	files := make([]*crdt.File, 0, len(paths))
	for _, p := range paths {
		if f, ok := m.doc.File(p); ok {
			files = append(files, f)
		}
	}
	return files
}

func (m *Mesh) snapshotAddrsLocked() map[priority.PeerID]string {
	out := make(map[priority.PeerID]string, len(m.addrs)+1)
	for p, a := range m.addrs {
		out[p] = a
	}
	out[m.self] = m.ln.Addr().String()
	return out
}

func (m *Mesh) snapshotAddrs() map[priority.PeerID]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotAddrsLocked()
}

func (m *Mesh) peerListMsg() PeerListMsg {
	return PeerListMsg{Addrs: m.snapshotAddrs(), Priorities: m.prio.Snapshot()}
}

// reconcilePeerList is the gossip-driven self-healing step: diff incoming
// addrs against what we're connected to, and (re)dial anything missing,
// with a small jittered delay to avoid a reconnect storm after a blip.
func (m *Mesh) reconcilePeerList(pl PeerListMsg) {
	for peer, n := range pl.Priorities {
		m.prio.Set(peer, n)
	}

	m.mu.Lock()
	var missing []struct {
		peer priority.PeerID
		addr string
	}
	for peer, addr := range pl.Addrs {
		if peer == m.self || addr == "" {
			continue
		}
		if _, ok := m.conns[peer]; !ok {
			missing = append(missing, struct {
				peer priority.PeerID
				addr string
			}{peer, addr})
		}
	}
	m.mu.Unlock()

	for _, p := range missing {
		peer, addr := p.peer, p.addr
		go func() {
			time.Sleep(time.Duration(rand.N(int64(200 * time.Millisecond))))
			m.dialPeer(peer, addr)
		}()
	}
}

func (m *Mesh) gossipLoop() {
	ticker := time.NewTicker(m.gossipInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.Broadcast(mustEnvelope(MsgPeerList, m.peerListMsg()))
		case <-m.done:
			return
		}
	}
}

// Broadcast is a best-effort fan-out of env to every connected peer.
func (m *Mesh) Broadcast(env Envelope) {
	m.mu.Lock()
	writers := make([]*connWriter, 0, len(m.conns))
	for _, cw := range m.conns {
		writers = append(writers, cw)
	}
	m.mu.Unlock()

	for _, cw := range writers {
		if err := cw.send(env); err != nil {
			log.Printf("network: broadcast to peer failed: %v", err)
		}
	}
}

func (m *Mesh) broadcastExcept(skip priority.PeerID, env Envelope) {
	m.mu.Lock()
	writers := make([]*connWriter, 0, len(m.conns))
	for peer, cw := range m.conns {
		if peer != skip {
			writers = append(writers, cw)
		}
	}
	m.mu.Unlock()

	for _, cw := range writers {
		_ = cw.send(env)
	}
}

// BroadcastDelta sends the resolved content of files to every connected
// peer as a MsgCRDTDelta, after a local sync round settles.
func (m *Mesh) BroadcastDelta(files []*crdt.File) {
	if len(files) == 0 {
		return
	}
	m.Broadcast(mustEnvelope(MsgCRDTDelta, CRDTFilesMsg{Files: files}))
}

// Incoming yields decoded, already-merged-into-doc remote file batches for
// pkg/session to react to (e.g. re-arm its commit debounce).
func (m *Mesh) Incoming() <-chan Delta { return m.incoming }

func (m *Mesh) Close() error {
	m.closeOne.Do(func() { close(m.done) })
	return m.ln.Close()
}

func mustEnvelope(t MsgType, v any) Envelope {
	env, err := encodeEnvelope(t, v)
	if err != nil {
		// Only ever fails for a non-gob-encodable type, which is a
		// programming error in this package, not a runtime condition.
		panic(err)
	}
	return env
}
