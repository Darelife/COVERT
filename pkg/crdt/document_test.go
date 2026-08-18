package crdt

import (
	"reflect"
	"testing"
)

func materializeText(d *Document, priorities map[string]int) []string {
	res := d.Materialize(priorities)
	out := make([]string, len(res))
	for i, l := range res {
		out[i] = l.Content
	}
	return out
}

func TestBasicInsertAndEdit(t *testing.T) {
	doc := NewDocument()
	priorities := map[string]int{"alice": 1}
	clock := &Clock{}

	ops := doc.ReconcileLocal("f.txt", []string{"hello", "world"}, "alice", priorities, clock)
	for _, op := range ops {
		doc.Apply(op.ID, op.Proposal)
	}
	if got := materializeText(doc, priorities); !reflect.DeepEqual(got, []string{"hello", "world"}) {
		t.Fatalf("got %v", got)
	}

	ops = doc.ReconcileLocal("f.txt", []string{"hello", "there", "world"}, "alice", priorities, clock)
	for _, op := range ops {
		doc.Apply(op.ID, op.Proposal)
	}
	if got := materializeText(doc, priorities); !reflect.DeepEqual(got, []string{"hello", "there", "world"}) {
		t.Fatalf("got %v", got)
	}
}

func TestConcurrentInsertsBothSurvive(t *testing.T) {
	doc := NewDocument()
	priorities := map[string]int{"alice": 1, "bob": 2}
	clockA := &Clock{}
	clockB := &Clock{}

	base := doc.ReconcileLocal("f.txt", []string{"line1"}, "alice", priorities, clockA)
	for _, op := range base {
		doc.Apply(op.ID, op.Proposal)
	}

	// Both peers concurrently insert a new second line after "line1", starting
	// from the same materialized base, without seeing each other's op yet.
	opsA := doc.ReconcileLocal("f.txt", []string{"line1", "from-alice"}, "alice", priorities, clockA)
	opsB := doc.ReconcileLocal("f.txt", []string{"line1", "from-bob"}, "bob", priorities, clockB)

	for _, op := range opsA {
		doc.Apply(op.ID, op.Proposal)
	}
	for _, op := range opsB {
		doc.Apply(op.ID, op.Proposal)
	}

	got := materializeText(doc, priorities)
	if len(got) != 3 {
		t.Fatalf("expected both concurrent inserts to survive, got %v", got)
	}

	// Applying in the opposite order must converge to the same result (commutativity).
	doc2 := NewDocument()
	for _, op := range base {
		doc2.Apply(op.ID, op.Proposal)
	}
	for _, op := range opsB {
		doc2.Apply(op.ID, op.Proposal)
	}
	for _, op := range opsA {
		doc2.Apply(op.ID, op.Proposal)
	}
	got2 := materializeText(doc2, priorities)
	if !reflect.DeepEqual(got, got2) {
		t.Fatalf("merge not commutative: %v vs %v", got, got2)
	}
}

func TestMajorityVoteWins(t *testing.T) {
	doc := NewDocument()
	priorities := map[string]int{"a": 1, "b": 2, "c": 3}
	clock := &Clock{}

	base := doc.ReconcileLocal("f.txt", []string{"orig"}, "a", priorities, clock)
	for _, op := range base {
		doc.Apply(op.ID, op.Proposal)
	}
	lineID := base[0].ID

	// b and c both propose "wins", a proposes "loses" -> majority (2/3) wins.
	doc.Apply(lineID, Proposal{Peer: "a", Seq: 100, Content: "loses"})
	doc.Apply(lineID, Proposal{Peer: "b", Seq: 100, Content: "wins"})
	doc.Apply(lineID, Proposal{Peer: "c", Seq: 100, Content: "wins"})

	got := materializeText(doc, priorities)
	if !reflect.DeepEqual(got, []string{"wins"}) {
		t.Fatalf("expected majority vote to win, got %v", got)
	}
}

func TestPriorityChainBreaksNoMajority(t *testing.T) {
	doc := NewDocument()
	// a has best (lowest) priority number.
	priorities := map[string]int{"a": 1, "b": 2, "c": 3}
	clock := &Clock{}

	base := doc.ReconcileLocal("f.txt", []string{"orig"}, "a", priorities, clock)
	for _, op := range base {
		doc.Apply(op.ID, op.Proposal)
	}
	lineID := base[0].ID

	// Three-way split, nobody has >50%: falls back to priority chain -> "a" wins.
	doc.Apply(lineID, Proposal{Peer: "a", Seq: 100, Content: "from-a"})
	doc.Apply(lineID, Proposal{Peer: "b", Seq: 100, Content: "from-b"})
	doc.Apply(lineID, Proposal{Peer: "c", Seq: 100, Content: "from-c"})

	got := materializeText(doc, priorities)
	if !reflect.DeepEqual(got, []string{"from-a"}) {
		t.Fatalf("expected priority chain to pick peer a, got %v", got)
	}
}

func TestRejoinDropsPriority(t *testing.T) {
	doc := NewDocument()
	clock := &Clock{}

	base := doc.ReconcileLocal("f.txt", []string{"orig"}, "a", map[string]int{"a": 1, "b": 2}, clock)
	for _, op := range base {
		doc.Apply(op.ID, op.Proposal)
	}
	lineID := base[0].ID

	doc.Apply(lineID, Proposal{Peer: "a", Seq: 100, Content: "from-a"})
	doc.Apply(lineID, Proposal{Peer: "b", Seq: 100, Content: "from-b"})

	// Before rejoin: a wins (priority 1 < 2).
	if got := materializeText(doc, map[string]int{"a": 1, "b": 2}); got[0] != "from-a" {
		t.Fatalf("expected from-a before rejoin, got %v", got)
	}

	// a disconnects and rejoins -> demoted to worst priority.
	if got := materializeText(doc, map[string]int{"a": 99, "b": 2}); got[0] != "from-b" {
		t.Fatalf("expected from-b after a's rejoin demotion, got %v", got)
	}
}

func TestConcurrentSameLineEditGoesThroughVote(t *testing.T) {
	doc := NewDocument()
	priorities := map[string]int{"a": 1, "b": 2, "c": 3}
	clockA, clockB, clockC := &Clock{}, &Clock{}, &Clock{}

	base := doc.ReconcileLocal("f.txt", []string{"shared line"}, "a", priorities, clockA)
	for _, op := range base {
		doc.Apply(op.ID, op.Proposal)
	}

	// a, b, and c each independently edit the same line differently, without
	// having seen each other's edit yet (simulates concurrent local watcher
	// callbacks before any network round-trip).
	opsA := doc.ReconcileLocal("f.txt", []string{"from-a"}, "a", priorities, clockA)
	opsB := doc.ReconcileLocal("f.txt", []string{"from-b"}, "b", priorities, clockB)
	opsC := doc.ReconcileLocal("f.txt", []string{"from-b"}, "c", priorities, clockC)

	if len(opsA) != 1 || len(opsB) != 1 || len(opsC) != 1 {
		t.Fatalf("expected a same-line edit to produce exactly one replace op each, got %d/%d/%d", len(opsA), len(opsB), len(opsC))
	}
	if opsA[0].ID != opsB[0].ID || opsB[0].ID != opsC[0].ID {
		t.Fatalf("expected all three edits to target the same LineID, got %v / %v / %v", opsA[0].ID, opsB[0].ID, opsC[0].ID)
	}

	for _, op := range opsA {
		doc.Apply(op.ID, op.Proposal)
	}
	for _, op := range opsB {
		doc.Apply(op.ID, op.Proposal)
	}
	for _, op := range opsC {
		doc.Apply(op.ID, op.Proposal)
	}

	// b and c agree ("from-b"): 2/3 is a majority, so it wins outright even
	// though a has the best priority.
	got := materializeText(doc, priorities)
	if !reflect.DeepEqual(got, []string{"from-b"}) {
		t.Fatalf("expected majority value from-b to win a same-line conflict, got %v", got)
	}
}

func TestDeleteWinsOverEdit(t *testing.T) {
	doc := NewDocument()
	clock := &Clock{}
	priorities := map[string]int{"a": 1, "b": 2, "c": 3}

	base := doc.ReconcileLocal("f.txt", []string{"orig"}, "a", priorities, clock)
	for _, op := range base {
		doc.Apply(op.ID, op.Proposal)
	}
	lineID := base[0].ID

	doc.Apply(lineID, Proposal{Peer: "a", Seq: 100, Deleted: true})
	doc.Apply(lineID, Proposal{Peer: "b", Seq: 100, Deleted: true})
	doc.Apply(lineID, Proposal{Peer: "c", Seq: 100, Content: "keep-me"})

	got := materializeText(doc, priorities)
	if len(got) != 0 {
		t.Fatalf("expected line deleted by majority, got %v", got)
	}
}

func TestStaleProposalIgnored(t *testing.T) {
	doc := NewDocument()
	clock := &Clock{}
	priorities := map[string]int{"a": 1}

	base := doc.ReconcileLocal("f.txt", []string{"orig"}, "a", priorities, clock)
	for _, op := range base {
		doc.Apply(op.ID, op.Proposal)
	}
	lineID := base[0].ID

	changed := doc.Apply(lineID, Proposal{Peer: "a", Seq: 5, Content: "newer"})
	if !changed {
		t.Fatalf("expected first bump to apply")
	}
	changed = doc.Apply(lineID, Proposal{Peer: "a", Seq: 3, Content: "older-out-of-order"})
	if changed {
		t.Fatalf("stale (lower-Seq) proposal should be ignored")
	}
	got := materializeText(doc, priorities)
	if got[0] != "newer" {
		t.Fatalf("expected newer to stick, got %v", got)
	}
}
