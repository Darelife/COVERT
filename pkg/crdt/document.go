package crdt

import (
	"math"
	"sort"
	"sync"
)

// Proposal is one peer's opinion about what a line should contain (or that it
// should be deleted). A line accumulates at most one live proposal per peer;
// a newer proposal (higher Seq) from the same peer replaces their older one.
type Proposal struct {
	Peer    string
	Seq     uint64
	Deleted bool
	Content string
}

// Line is a single CRDT element. Its ID fixes its structural position in the
// document forever (inserts are commutative and never conflict); its content
// is decided at materialization time by voting across live Proposals.
type Line struct {
	ID        LineID
	Proposals map[string]Proposal // peer -> that peer's current proposal
}

// ResolvedLine is a materialized (ID, winning content) pair.
type ResolvedLine struct {
	ID      LineID
	Content string
}

// Document is the CRDT state for a single file: a set of Lines, ordered by
// LineID, each with a set of competing Proposals resolved on read.
type Document struct {
	mu    sync.Mutex
	Lines map[string]*Line
}

func NewDocument() *Document {
	return &Document{Lines: make(map[string]*Line)}
}

// Apply merges one (LineID, Proposal) pair into the document. It is the sole
// mutation entrypoint and is idempotent/commutative, so it's safe to call for
// both locally generated ops and ops received from any peer in any order.
// Returns true if this changed document state.
func (d *Document) Apply(id LineID, p Proposal) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := id.String()
	line, ok := d.Lines[key]
	if !ok {
		line = &Line{ID: id, Proposals: make(map[string]Proposal)}
		d.Lines[key] = line
	}

	if existing, ok := line.Proposals[p.Peer]; ok && existing.Seq >= p.Seq {
		return false
	}
	line.Proposals[p.Peer] = p
	return true
}

// sortedLines returns all lines (including fully-tombstoned ones) in document order.
func (d *Document) sortedLines() []*Line {
	lines := make([]*Line, 0, len(d.Lines))
	for _, l := range d.Lines {
		lines = append(lines, l)
	}
	sort.Slice(lines, func(i, j int) bool { return lines[i].ID.Less(lines[j].ID) })
	return lines
}

// ForEach calls fn once per currently known (LineID, Proposal) pair, e.g. to
// replicate this document's full state to a newly connected peer.
func (d *Document) ForEach(fn func(LineID, Proposal)) {
	d.mu.Lock()
	lines := d.sortedLines()
	d.mu.Unlock()

	for _, l := range lines {
		for _, p := range l.Proposals {
			fn(l.ID, p)
		}
	}
}

// Materialize resolves every line's competing proposals and returns the
// surviving (non-deleted) lines in document order. priorities maps peer ID ->
// join-priority (lower number = higher priority); a peer missing from the map
// is treated as lowest priority.
func (d *Document) Materialize(priorities map[string]int) []ResolvedLine {
	d.mu.Lock()
	lines := d.sortedLines()
	d.mu.Unlock()

	out := make([]ResolvedLine, 0, len(lines))
	for _, l := range lines {
		winner := resolveLine(l, priorities)
		if winner.Deleted {
			continue
		}
		out = append(out, ResolvedLine{ID: l.ID, Content: winner.Content})
	}
	return out
}

// resolveLine picks the winning Proposal for a line:
//  1. Group proposals by their resulting value (content or delete).
//  2. If one value has strictly more than half the votes among peers who
//     have an opinion on THIS line, it wins outright (majority rule).
//  3. Otherwise (tie, or no value clears 50%), fall back to the proposal from
//     whichever peer currently has the best (numerically lowest) priority.
func resolveLine(line *Line, priorities map[string]int) Proposal {
	type voteKey struct {
		deleted bool
		content string
	}
	votes := make(map[voteKey]int)
	sample := make(map[voteKey]Proposal)
	total := len(line.Proposals)

	for _, p := range line.Proposals {
		k := voteKey{p.Deleted, p.Content}
		votes[k]++
		if _, ok := sample[k]; !ok {
			sample[k] = p
		}
	}

	for k, count := range votes {
		if count*2 > total {
			return sample[k]
		}
	}

	var winner Proposal
	bestPriority := math.MaxInt
	first := true
	for _, p := range line.Proposals {
		pr, ok := priorities[p.Peer]
		if !ok {
			pr = math.MaxInt
		}
		if first || pr < bestPriority || (pr == bestPriority && p.Peer < winner.Peer) {
			winner = p
			bestPriority = pr
			first = false
		}
	}
	return winner
}
