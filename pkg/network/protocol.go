package network

import "covert/pkg/crdt"

type msgType string

const (
	msgJoinRequest  msgType = "join_request"  // sent once, only when actively joining/rejoining a session
	msgJoinAccepted msgType = "join_accepted" // reply: your assigned priority + known peer list
	msgHello        msgType = "hello"         // routine mesh-link identification, no reassignment
	msgPriority     msgType = "priority"      // gossip: peerID now has this priority (+ its address)
	msgOp           msgType = "op"            // one CRDT op
)

// envelope is the single wire message type, newline-delimited JSON.
type envelope struct {
	Type msgType `json:"type"`

	PeerID     string `json:"peer_id,omitempty"`
	ListenAddr string `json:"listen_addr,omitempty"`
	Priority   int    `json:"priority,omitempty"`

	Peers      map[string]string `json:"peers,omitempty"`      // peerID -> listen addr, sent with join_accepted
	Priorities map[string]int    `json:"priorities,omitempty"` // peerID -> priority, sent with join_accepted

	Op *crdt.Op `json:"op,omitempty"`
}
