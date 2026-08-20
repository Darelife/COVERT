package crdt

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// fakePriority is a minimal PriorityLookup for tests: lower number wins,
// unknown peers sort last (matches pkg/priority.Table.Lookup's contract).
type fakePriority map[PeerID]int

func (f fakePriority) Lookup(p PeerID) int {
	if n, ok := f[p]; ok {
		return n
	}
	return math.MaxInt
}

func TestFracIndexCompareBasic(t *testing.T) {
	require.Equal(t, -1, Compare(FracIndex{1}, FracIndex{2}))
	require.Equal(t, 1, Compare(FracIndex{2}, FracIndex{1}))
	require.Equal(t, 0, Compare(FracIndex{1, 0}, FracIndex{1}))
	require.Equal(t, -1, Compare(Begin, FracIndex{1}))
	require.Equal(t, -1, Compare(FracIndex{1 << 20}, End))
}

func TestGenerateBetweenStaysStrictlyBetween(t *testing.T) {
	a := FracIndex{10}
	b := FracIndex{20}
	for i := 0; i < 200; i++ {
		mid := GenerateBetween(a, b, "peer")
		require.Equal(t, -1, Compare(a, mid), "a=%v mid=%v", a, mid)
		require.Equal(t, -1, Compare(mid, b), "mid=%v b=%v", mid, b)
	}
}

func TestGenerateBetweenAdjacentDigitsRecursesDeeper(t *testing.T) {
	a := FracIndex{5}
	b := FracIndex{6} // adjacent: no room at depth 0
	for i := 0; i < 50; i++ {
		mid := GenerateBetween(a, b, "peer")
		require.Equal(t, -1, Compare(a, mid))
		require.Equal(t, -1, Compare(mid, b))
	}
}

func TestGenerateBetweenAtBoundaries(t *testing.T) {
	// Inserting the very first line in an empty file.
	first := GenerateBetween(Begin, End, "peer")
	require.Equal(t, -1, Compare(Begin, first))
	require.Equal(t, -1, Compare(first, End))

	// Inserting after the last line.
	last := FracIndex{100}
	after := GenerateBetween(last, End, "peer")
	require.Equal(t, -1, Compare(last, after))
	require.Equal(t, -1, Compare(after, End))

	// Inserting before the first line.
	before := GenerateBetween(Begin, last, "peer")
	require.Equal(t, -1, Compare(Begin, before))
	require.Equal(t, -1, Compare(before, last))
}

func TestGenerateBetweenTwoPeersDivergeAtSameSpot(t *testing.T) {
	a, b := FracIndex{5}, FracIndex{6}
	for i := 0; i < 100; i++ {
		alice := GenerateBetween(a, b, "alice")
		bob := GenerateBetween(a, b, "bob")
		require.NotEqual(t, alice, bob, "two independent inserts at the same spot must not collide")
	}
}

// --- Register.Resolve: vote-then-priority -----------------------------

func TestRegisterMajorityVoteWins(t *testing.T) {
	reg := newRegister()
	reg.Proposals["a"] = Proposal{Peer: "a", Value: "x"}
	reg.Proposals["b"] = Proposal{Peer: "b", Value: "x"}
	reg.Proposals["c"] = Proposal{Peer: "c", Value: "y"}

	winner := reg.Resolve(fakePriority{"a": 1, "b": 2, "c": 3})
	require.Equal(t, "x", winner.Value)
}

func TestRegisterTieFallsBackToPriority(t *testing.T) {
	reg := newRegister()
	reg.Proposals["a"] = Proposal{Peer: "a", Value: "x"}
	reg.Proposals["b"] = Proposal{Peer: "b", Value: "y"}

	// No majority (1-1 tie among 2 voters): best priority wins.
	winner := reg.Resolve(fakePriority{"a": 5, "b": 1})
	require.Equal(t, "y", winner.Value)

	winner = reg.Resolve(fakePriority{"a": 1, "b": 5})
	require.Equal(t, "x", winner.Value)
}

func TestRegisterNoMajorityThreeWayFallsBackToPriority(t *testing.T) {
	reg := newRegister()
	reg.Proposals["a"] = Proposal{Peer: "a", Value: "x"}
	reg.Proposals["b"] = Proposal{Peer: "b", Value: "y"}
	reg.Proposals["c"] = Proposal{Peer: "c", Value: "z"}

	winner := reg.Resolve(fakePriority{"a": 2, "b": 1, "c": 3})
	require.Equal(t, "y", winner.Value)
}

func TestRegisterUnknownPeerNeverWinsTiebreak(t *testing.T) {
	reg := newRegister()
	reg.Proposals["known"] = Proposal{Peer: "known", Value: "x"}
	reg.Proposals["stranger"] = Proposal{Peer: "stranger", Value: "y"}

	winner := reg.Resolve(fakePriority{"known": 5}) // stranger unknown -> MaxInt
	require.Equal(t, "x", winner.Value)
}

// --- Concurrent structural inserts are commutative ---------------------

func TestConcurrentInsertsCommute(t *testing.T) {
	// Both peers independently allocate their position against the SAME
	// shared neighbors (Begin/End) before either has seen the other's
	// insert — that's what "concurrent" means here. The resulting
	// FracIndex values are fixed once, then applied in both orders below.
	id1 := LineID{Pos: GenerateBetween(Begin, End, "alice"), Peer: "alice"}
	id2 := LineID{Pos: GenerateBetween(Begin, End, "bob"), Peer: "bob"}
	ids := map[string]LineID{"alice": id1, "bob": id2}

	apply := func(order []string) *File {
		f := NewFile("doc.txt")
		for _, who := range order {
			f.InsertLine(ids[who], PeerID(who), who+"-line")
		}
		return f
	}

	f1 := apply([]string{"alice", "bob"})
	f2 := apply([]string{"bob", "alice"})

	require.Len(t, f1.Order, 2)
	require.Len(t, f2.Order, 2)
	// Applying in either order converges to the same structural order,
	// since GenerateBetween results only depend on the shared neighbors,
	// not on application order.
	require.Equal(t, f1.Order, f2.Order)
}

// --- Same-line concurrent edits contend, they don't fork ---------------

func TestSameLineConcurrentEditsContendAsProposals(t *testing.T) {
	f := NewFile("doc.txt")
	id := LineID{Pos: GenerateBetween(Begin, End, "alice"), Peer: "alice"}
	f.InsertLine(id, "alice", "original")

	// Two peers concurrently propose different content for the SAME line.
	f.ProposeLineEdit(id, "alice", "alice's edit")
	f.ProposeLineEdit(id, "bob", "bob's edit")

	require.Len(t, f.Lines, 1, "must still be exactly one line, not two")
	winner := f.Lines[id.key()].Reg.Resolve(fakePriority{"alice": 1, "bob": 2})
	require.Equal(t, "alice's edit", winner.Value, "alice has better priority, tie broken her way")
}

// --- Priority-chain tiebreak & rejoin demotion --------------------------

func TestPriorityChainTiebreakReResolvesLiveOnPriorityChange(t *testing.T) {
	reg := newRegister()
	reg.Proposals["alice"] = Proposal{Peer: "alice", Value: "a-value"}
	reg.Proposals["bob"] = Proposal{Peer: "bob", Value: "b-value"}

	prio := fakePriority{"alice": 1, "bob": 2}
	require.Equal(t, "a-value", reg.Resolve(prio).Value)

	// Simulate alice rejoining: her priority number gets strictly worse
	// (demoted), re-resolving the same still-open register differently —
	// priority is looked up live, never baked into the proposal.
	prio["alice"] = 3
	require.Equal(t, "b-value", reg.Resolve(prio).Value)
}

// --- File create/delete/existence reuse the same vote-then-priority rule --

func TestConcurrentCreateSamePathDifferentContentResolvesByPriority(t *testing.T) {
	f := NewFile("new.txt")
	f.Reg.Proposals["alice"] = Proposal{Peer: "alice", Value: "alice-created"}
	f.Reg.Proposals["bob"] = Proposal{Peer: "bob", Value: "bob-created"}

	winner := f.ResolveExistence(fakePriority{"alice": 1, "bob": 2})
	require.False(t, winner.Tombstone)
	require.Equal(t, "alice-created", winner.Value)
}

func TestDeleteBeatsStaleEditFromEarlierRound(t *testing.T) {
	f := NewFile("doc.txt")
	f.RefreshExistence("alice") // both peers already agree the file exists
	f.RefreshExistence("bob")

	// alice proposes delete, having observed the current (stale) version.
	f.ProposeDelete("alice")

	winner := f.ResolveExistence(fakePriority{"alice": 1, "bob": 2})
	require.True(t, winner.Tombstone, "delete should win: bob's edit predates it")
}

func TestConcurrentEditBeatsDelete(t *testing.T) {
	f := NewFile("doc.txt")
	f.RefreshExistence("alice")
	f.RefreshExistence("bob")

	f.ProposeDelete("alice")

	// bob's edit happens concurrently (after the delete was proposed,
	// unaware of it) — this must bump Version and refresh bob's existence
	// claim past what the delete observed.
	f.Version++
	f.RefreshExistence("bob")

	winner := f.ResolveExistence(fakePriority{"alice": 1, "bob": 2})
	require.False(t, winner.Tombstone, "concurrent edit must beat the delete")
}

func TestResetVersionZeroesCounters(t *testing.T) {
	doc := NewDocument()
	f := doc.GetOrCreateFile("doc.txt")
	f.Version = 7
	f.DeleteObservedAt = 3

	doc.ResetVersion("doc.txt")

	require.Equal(t, uint64(0), f.Version)
	require.Equal(t, uint64(0), f.DeleteObservedAt)
}

// --- MaterializeContent -------------------------------------------------

func TestMaterializeContentSkipsTombstonedLines(t *testing.T) {
	f := NewFile("doc.txt")
	id1 := LineID{Pos: GenerateBetween(Begin, End, "alice"), Peer: "alice"}
	f.InsertLine(id1, "alice", "line one")
	id2 := LineID{Pos: GenerateBetween(id1.Pos, End, "alice"), Peer: "alice"}
	f.InsertLine(id2, "alice", "line two")

	f.ProposeLineDelete(id1, "alice")

	prio := fakePriority{"alice": 1}
	require.Equal(t, "line two", f.MaterializeContent(prio))
}

// --- ApplyRemote: proposal-wise union, order-independent ----------------

func TestApplyRemoteUnionsProposalsRegardlessOfOrder(t *testing.T) {
	build := func() *Document { return NewDocument() }

	// Peer A's local view: created the file, wrote one line.
	local := build()
	lf := local.GetOrCreateFile("doc.txt")
	lf.RefreshExistence("alice")
	id := LineID{Pos: GenerateBetween(Begin, End, "alice"), Peer: "alice"}
	lf.InsertLine(id, "alice", "alice's line")

	// Peer B's remote delta: same file, concurrent edit to the same line.
	remote := NewFile("doc.txt")
	remote.RefreshExistence("bob")
	remote.Lines[id.key()] = &Line{ID: id, Reg: Register{Proposals: map[PeerID]Proposal{
		"bob": {Peer: "bob", Value: "bob's line"},
	}}}

	local.ApplyRemote(remote)

	merged, ok := local.File("doc.txt")
	require.True(t, ok)
	require.Len(t, merged.Lines, 1, "delta arriving out of order must not fork the line")
	reg := merged.Lines[id.key()].Reg
	require.Contains(t, reg.Proposals, PeerID("alice"))
	require.Contains(t, reg.Proposals, PeerID("bob"))
}
