// Package network implements a simple full-mesh P2P transport: every peer
// connects directly to every other peer over TCP. There is no relay/flooding
// of messages - a locally generated op is sent once to each direct peer -
// which is sufficient because the mesh is kept fully connected via peer
// exchange on join.
package network

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"covert/pkg/crdt"
	"covert/pkg/priority"
)

type peerConn struct {
	id   string
	conn net.Conn
	enc  *json.Encoder
	mu   sync.Mutex
}

func (c *peerConn) send(e envelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.enc.Encode(e)
}

type Node struct {
	SelfID     string
	ListenAddr string // address advertised to peers for them to dial us at

	Registry *priority.Registry

	OnOp       func(op crdt.Op)
	OnPeerUp   func(peerID string)
	OnPeerDown func(peerID string)

	mu    sync.Mutex
	conns map[string]*peerConn
	addrs map[string]string
}

func NewNode(selfID string, reg *priority.Registry) *Node {
	return &Node{
		SelfID:   selfID,
		Registry: reg,
		conns:    make(map[string]*peerConn),
		addrs:    make(map[string]string),
	}
}

// Listen binds bindAddr (which may use port 0 for an OS-assigned port) and
// returns the actual address peers should be told to dial, which also
// becomes n.ListenAddr.
func (n *Node) Listen(bindAddr string) (string, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return "", err
	}
	n.ListenAddr = ln.Addr().String()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go n.serveInbound(c)
		}
	}()
	return n.ListenAddr, nil
}

// Join dials an existing session member, requests a (demoted-if-returning)
// priority assignment, and mesh-connects to every peer it learns about.
func (n *Node) Join(dialAddr string) error {
	c, err := net.Dial("tcp", dialAddr)
	if err != nil {
		return fmt.Errorf("dial %s: %w", dialAddr, err)
	}
	dec := json.NewDecoder(c)
	enc := json.NewEncoder(c)

	if err := enc.Encode(envelope{Type: msgJoinRequest, PeerID: n.SelfID, ListenAddr: n.ListenAddr}); err != nil {
		return err
	}

	var reply envelope
	if err := dec.Decode(&reply); err != nil {
		return fmt.Errorf("reading join_accepted: %w", err)
	}
	if reply.Type != msgJoinAccepted {
		return fmt.Errorf("unexpected reply type %q to join request", reply.Type)
	}

	n.Registry.Observe(n.SelfID, reply.Priority)
	responderID := reply.PeerID
	n.addAddr(responderID, dialAddr)
	pc := &peerConn{id: responderID, conn: c, enc: enc}
	if !n.addConn(pc) {
		c.Close()
		return fmt.Errorf("already connected to %s", responderID)
	}
	if n.OnPeerUp != nil {
		n.OnPeerUp(responderID)
	}

	for peerID, pr := range reply.Priorities {
		n.Registry.Observe(peerID, pr)
	}
	for peerID, addr := range reply.Peers {
		if peerID == n.SelfID || peerID == responderID {
			continue
		}
		n.addAddr(peerID, addr)
		n.ensureMeshLink(peerID, addr)
	}

	go n.serveOngoing(pc, dec)
	return nil
}

// serveInbound handles a freshly accepted connection whose remote identity
// isn't known yet: the first message must be a join_request (a new/rejoining
// peer) or a hello (a mesh-repair link from an already-known peer).
func (n *Node) serveInbound(c net.Conn) {
	dec := json.NewDecoder(c)
	enc := json.NewEncoder(c)

	var env envelope
	if err := dec.Decode(&env); err != nil {
		c.Close()
		return
	}

	var pc *peerConn
	switch env.Type {
	case msgJoinRequest:
		peerID := env.PeerID
		pr := n.Registry.Assign(peerID)
		n.addAddr(peerID, env.ListenAddr)
		pc = &peerConn{id: peerID, conn: c, enc: enc}
		if !n.addConn(pc) {
			c.Close()
			return
		}

		reply := envelope{
			Type:       msgJoinAccepted,
			PeerID:     n.SelfID,
			Priority:   pr,
			Peers:      n.snapshotAddrs(),
			Priorities: n.Registry.Snapshot(),
		}
		if err := pc.send(reply); err != nil {
			n.dropConn(pc)
			return
		}
		n.broadcastExcept(peerID, envelope{Type: msgPriority, PeerID: peerID, Priority: pr, ListenAddr: env.ListenAddr})

	case msgHello:
		peerID := env.PeerID
		n.addAddr(peerID, env.ListenAddr)
		pc = &peerConn{id: peerID, conn: c, enc: enc}
		if !n.addConn(pc) {
			c.Close()
			return
		}
		// Reply in kind so the dialer can confirm our identity: an address
		// learned via gossip may since have been reused by a different
		// (e.g. rejoined) peer, and the dialer must not silently register
		// this link under the wrong ID.
		if err := pc.send(envelope{Type: msgHello, PeerID: n.SelfID, ListenAddr: n.ListenAddr}); err != nil {
			n.dropConn(pc)
			return
		}

	default:
		c.Close()
		return
	}

	if n.OnPeerUp != nil {
		n.OnPeerUp(pc.id)
	}
	n.serveOngoing(pc, dec)
}

// dialMesh proactively connects to a peer learned about via gossip, so the
// mesh stays fully connected without routing every op through a relay.
// expectedPeerID is only a hint: addresses get reused (e.g. a rejoin on the
// same port), so the connection is registered under whatever ID the far end
// actually presents, not the one we expected to find there.
func (n *Node) dialMesh(addr, expectedPeerID string) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return
	}
	enc := json.NewEncoder(c)
	dec := json.NewDecoder(c)

	if err := enc.Encode(envelope{Type: msgHello, PeerID: n.SelfID, ListenAddr: n.ListenAddr}); err != nil {
		c.Close()
		return
	}
	var reply envelope
	if err := dec.Decode(&reply); err != nil || reply.Type != msgHello {
		c.Close()
		return
	}
	actualID := reply.PeerID
	if actualID != expectedPeerID {
		n.addAddr(actualID, addr)
	}

	pc := &peerConn{id: actualID, conn: c, enc: enc}
	if !n.addConn(pc) {
		c.Close()
		return
	}
	if n.OnPeerUp != nil {
		n.OnPeerUp(actualID)
	}
	n.serveOngoing(pc, dec)
}

func (n *Node) ensureMeshLink(peerID, addr string) {
	if peerID == n.SelfID || addr == "" || n.hasConn(peerID) {
		return
	}
	go n.dialMesh(addr, peerID)
}

func (n *Node) serveOngoing(pc *peerConn, dec *json.Decoder) {
	for {
		var env envelope
		if err := dec.Decode(&env); err != nil {
			n.dropConn(pc)
			if n.OnPeerDown != nil {
				n.OnPeerDown(pc.id)
			}
			return
		}
		switch env.Type {
		case msgPriority:
			n.Registry.Observe(env.PeerID, env.Priority)
			if env.ListenAddr != "" {
				n.addAddr(env.PeerID, env.ListenAddr)
				n.ensureMeshLink(env.PeerID, env.ListenAddr)
			}
		case msgOp:
			if env.Op != nil && n.OnOp != nil {
				n.OnOp(*env.Op)
			}
		}
	}
}

// Broadcast sends a locally generated op directly to every connected peer.
func (n *Node) Broadcast(op crdt.Op) {
	n.broadcastExcept("", envelope{Type: msgOp, Op: &op})
}

// SendOp sends a single op to one specific peer (e.g. full-state replication
// to a newly connected peer), a no-op if that peer isn't currently connected.
func (n *Node) SendOp(peerID string, op crdt.Op) {
	n.mu.Lock()
	pc, ok := n.conns[peerID]
	n.mu.Unlock()
	if ok {
		pc.send(envelope{Type: msgOp, Op: &op})
	}
}

func (n *Node) broadcastExcept(except string, env envelope) {
	n.mu.Lock()
	targets := make([]*peerConn, 0, len(n.conns))
	for id, c := range n.conns {
		if id != except {
			targets = append(targets, c)
		}
	}
	n.mu.Unlock()
	for _, c := range targets {
		c.send(env)
	}
}

func (n *Node) addAddr(id, addr string) {
	if addr == "" {
		return
	}
	n.mu.Lock()
	n.addrs[id] = addr
	n.mu.Unlock()
}

// addConn registers pc as the connection for its peer ID, unless one is
// already registered (a race between, e.g., our own mesh-repair dial and the
// peer's gossip-triggered dial to us landing at the same time). Returns
// false if pc lost that race; the caller must close pc's underlying conn and
// abandon it rather than starting a read loop on an orphaned socket.
func (n *Node) addConn(pc *peerConn) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.conns[pc.id]; exists {
		return false
	}
	n.conns[pc.id] = pc
	return true
}

// dropConn removes pc from the connection table only if it is still the
// currently registered connection for its peer ID. This matters because a
// since-superseded duplicate connection erroring out must not evict a
// different, live connection that has since taken its place in the map.
func (n *Node) dropConn(pc *peerConn) {
	n.mu.Lock()
	if cur, ok := n.conns[pc.id]; ok && cur == pc {
		delete(n.conns, pc.id)
	}
	n.mu.Unlock()
}

func (n *Node) hasConn(id string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, ok := n.conns[id]
	return ok
}

func (n *Node) snapshotAddrs() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string]string, len(n.addrs)+1)
	for k, v := range n.addrs {
		out[k] = v
	}
	out[n.SelfID] = n.ListenAddr
	return out
}

// PeerCount returns the number of currently connected peers (not including self).
func (n *Node) PeerCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.conns)
}
