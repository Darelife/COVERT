package network

import (
	"bytes"
	"testing"
	"time"

	"github.com/darelife/covert/pkg/crdt"
	"github.com/darelife/covert/pkg/priority"
	"github.com/stretchr/testify/require"
)

// drainInto mimics pkg/session's applyRemote: network never merges
// incoming deltas into a Document itself (that's the caller's job, done on
// whatever single goroutine owns doc — see Mesh.deliver's doc comment), so
// tests that want to observe merged state need to drain Incoming() and
// apply it themselves, same as a real Session would.
// joinerPrioLookup adapts priority.Table to crdt.PriorityLookup, the same
// way pkg/session does, so tests can read resolved content through a
// locked accessor rather than reading crdt.File fields directly.
type joinerPrioLookup struct{ t *priority.Table }

func (p joinerPrioLookup) Lookup(id crdt.PeerID) int { return p.t.Lookup(priority.PeerID(id)) }

func drainInto(m *Mesh, doc *crdt.Document, stop <-chan struct{}) {
	for {
		select {
		case d := <-m.Incoming():
			for _, f := range d.Files {
				doc.ApplyRemote(f)
			}
		case <-stop:
			return
		}
	}
}

func TestFrameRoundTrip(t *testing.T) {
	env := mustEnvelope(MsgHello, HelloMsg{Peer: "alice", Addr: "127.0.0.1:1234"})

	var buf bytes.Buffer
	require.NoError(t, writeFrame(&buf, env))

	got, err := readFrame(&buf)
	require.NoError(t, err)
	require.Equal(t, env.Type, got.Type)

	var hello HelloMsg
	require.NoError(t, decodePayload(got.Payload, &hello))
	require.Equal(t, priority.PeerID("alice"), hello.Peer)
	require.Equal(t, "127.0.0.1:1234", hello.Addr)
}

func TestFrameRoundTripMultipleFramesOnSameStream(t *testing.T) {
	var buf bytes.Buffer
	envs := []Envelope{
		mustEnvelope(MsgHello, HelloMsg{Peer: "a", Addr: "x"}),
		mustEnvelope(MsgPeerList, PeerListMsg{Addrs: map[priority.PeerID]string{"a": "x"}}),
		mustEnvelope(MsgPriorityAssign, PriorityAssignMsg{Peer: "a", Number: 2}),
	}
	for _, e := range envs {
		require.NoError(t, writeFrame(&buf, e))
	}
	for _, want := range envs {
		got, err := readFrame(&buf)
		require.NoError(t, err)
		require.Equal(t, want.Type, got.Type)
	}
}

func TestJoinHandshakeConvergesPriorityAndSnapshot(t *testing.T) {
	founderID := priority.PeerID("founder")
	founderPrio := priority.New()
	founderDoc := crdt.NewDocument()
	founder, err := NewFounder(founderID, founderPrio, founderDoc, "127.0.0.1:0")
	require.NoError(t, err)
	defer founder.Close()
	require.Equal(t, 1, founderPrio.Lookup(founderID))

	// Seed content before the joiner arrives, to verify snapshot catch-up.
	f := founderDoc.GetOrCreateFile("hello.txt")
	f.RefreshExistence(crdt.PeerID(founderID))
	id := crdt.LineID{Pos: crdt.GenerateBetween(crdt.Begin, crdt.End, crdt.PeerID(founderID)), Peer: crdt.PeerID(founderID)}
	f.InsertLine(id, crdt.PeerID(founderID), "hello from founder")

	joinerID := priority.PeerID("joiner")
	joinerPrio := priority.New()
	joinerDoc := crdt.NewDocument()
	joiner, err := NewJoiner(joinerID, joinerPrio, joinerDoc, "127.0.0.1:0", founder.ListenAddr())
	require.NoError(t, err)
	defer joiner.Close()

	stop := make(chan struct{})
	defer close(stop)
	go drainInto(joiner, joinerDoc, stop)

	require.Equal(t, 2, joinerPrio.Lookup(joinerID), "joiner should be assigned priority 2 (founder is 1)")

	require.Eventually(t, func() bool {
		jf, ok := joinerDoc.File("hello.txt")
		if !ok {
			return false
		}
		return len(jf.ResolvedLines(joinerPrioLookup{joinerPrio})) == 1
	}, 2*time.Second, 20*time.Millisecond, "joiner should catch up on the founder's pre-existing content via the snapshot")

	require.Eventually(t, func() bool {
		return founderPrio.Lookup(joinerID) == 2
	}, 2*time.Second, 20*time.Millisecond, "founder assigned the number directly, should know it immediately")
}

func TestBroadcastDeltaReachesJoinedPeer(t *testing.T) {
	founderID := priority.PeerID("founder")
	founderPrio := priority.New()
	founderDoc := crdt.NewDocument()
	founder, err := NewFounder(founderID, founderPrio, founderDoc, "127.0.0.1:0")
	require.NoError(t, err)
	defer founder.Close()

	joinerID := priority.PeerID("joiner")
	joinerPrio := priority.New()
	joinerDoc := crdt.NewDocument()
	joiner, err := NewJoiner(joinerID, joinerPrio, joinerDoc, "127.0.0.1:0", founder.ListenAddr())
	require.NoError(t, err)
	defer joiner.Close()

	changed := founderDoc.GetOrCreateFile("later.txt")
	changed.RefreshExistence(crdt.PeerID(founderID))
	founder.BroadcastDelta([]*crdt.File{changed})

	select {
	case d := <-joiner.Incoming():
		require.Len(t, d.Files, 1)
		require.Equal(t, "later.txt", d.Files[0].Path)
		// Merging into joinerDoc is the receiver's job (pkg/session, in
		// production) — network only decodes and delivers.
		joinerDoc.ApplyRemote(d.Files[0])
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delta")
	}

	_, ok := joinerDoc.File("later.txt")
	require.True(t, ok)
}

func TestThreePeerMeshFullyConnects(t *testing.T) {
	founderID := priority.PeerID("founder")
	founderPrio := priority.New()
	founderDoc := crdt.NewDocument()
	founder, err := NewFounder(founderID, founderPrio, founderDoc, "127.0.0.1:0")
	require.NoError(t, err)
	defer founder.Close()

	aliceID := priority.PeerID("alice")
	alicePrio := priority.New()
	aliceDoc := crdt.NewDocument()
	alice, err := NewJoiner(aliceID, alicePrio, aliceDoc, "127.0.0.1:0", founder.ListenAddr())
	require.NoError(t, err)
	defer alice.Close()

	// Give the founder->alice gossip a moment before bob joins, so bob's
	// mesh-completion dial to alice doesn't race alice's own registration
	// with the founder (a documented POC limitation, not something this
	// test needs to stress).
	require.Eventually(t, func() bool { return founderPrio.Lookup(aliceID) == 2 }, time.Second, 10*time.Millisecond)

	bobID := priority.PeerID("bob")
	bobPrio := priority.New()
	bobDoc := crdt.NewDocument()
	bob, err := NewJoiner(bobID, bobPrio, bobDoc, "127.0.0.1:0", founder.ListenAddr())
	require.NoError(t, err)
	defer bob.Close()

	require.Equal(t, 3, bobPrio.Lookup(bobID))

	// bob should end up directly connected to alice too (full mesh), not
	// just to the founder. Verify by broadcasting from alice and checking
	// bob receives it directly.
	marker := aliceDoc.GetOrCreateFile("from-alice.txt")
	marker.RefreshExistence(crdt.PeerID(aliceID))

	require.Eventually(t, func() bool {
		alice.BroadcastDelta([]*crdt.File{marker})
		select {
		case d := <-bob.Incoming():
			return len(d.Files) == 1 && d.Files[0].Path == "from-alice.txt"
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}, 3*time.Second, 250*time.Millisecond, "bob should be directly reachable from alice in a full mesh")
}
