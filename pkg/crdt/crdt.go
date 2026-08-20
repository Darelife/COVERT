// Package crdt implements COVERT's conflict-resolution CRDT: how a line's
// content, and a file's existence, converge across peers. See this
// package's README for the design rationale (vote-then-priority, applied
// uniformly to line content and file/path existence).
package crdt

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

type PeerID string

// LineID is a line's permanent structural identity: a fractional-index
// position plus its creator's peer ID. Peer only breaks a same-Pos tie,
// which GenerateBetween is designed never to produce.
type LineID struct {
	Pos  FracIndex
	Peer PeerID
}

func lineIDLess(a, b LineID) bool {
	if c := Compare(a.Pos, b.Pos); c != 0 {
		return c < 0
	}
	return a.Peer < b.Peer
}

// key returns a comparable string encoding of the LineID, since FracIndex
// is a slice and Go map keys must be comparable. Each digit is fixed-width
// hex so the Pos prefix can never be confused with the Peer suffix.
func (id LineID) key() string {
	var sb strings.Builder
	for _, d := range id.Pos {
		fmt.Fprintf(&sb, "%08x.", d)
	}
	sb.WriteString(string(id.Peer))
	return sb.String()
}

// Proposal is one peer's claim about a register's value.
type Proposal struct {
	Peer      PeerID
	Value     string // line content; ignored when Tombstone is true
	Tombstone bool   // true = this peer proposes "line/path does not exist"
	Version   uint64 // the register owner's Version at the moment of proposing

	// Seq is a per-process, never-reset monotonic sequence number stamped
	// on every proposal this peer makes, used only by mergeRegister to
	// decide which of two copies of THIS peer's own proposal is newer.
	// Version can't serve that role: it's File.Version, which resets to 0
	// every commit round, so two proposals from different rounds can
	// share the same Version — without Seq, a peer re-broadcasting its
	// (possibly stale) cached copy of another peer's proposal could
	// overwrite that peer's own newer live proposal on merge, since a
	// Version tie always resolved to "overwrite".
	Seq uint64
}

var proposalSeq atomic.Uint64

func nextProposalSeq() uint64 { return proposalSeq.Add(1) }

// Register is a contested slot: at most one live proposal per peer.
type Register struct {
	Proposals map[PeerID]Proposal
}

func newRegister() Register {
	return Register{Proposals: map[PeerID]Proposal{}}
}

// PriorityLookup decouples this package from pkg/priority's concrete type.
type PriorityLookup interface {
	Lookup(PeerID) int // lower wins; unknown peers return math.MaxInt
}

func sortedPeers(m map[PeerID]Proposal) []PeerID {
	peers := make([]PeerID, 0, len(m))
	for p := range m {
		peers = append(peers, p)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })
	return peers
}

func proposalKey(p Proposal) string {
	if p.Tombstone {
		return "\x00"
	}
	return "\x01" + p.Value
}

// Resolve picks a register's winning proposal: strict majority vote first,
// falling back to whichever proposer currently holds the best (lowest)
// join-priority. Iteration is over peer IDs in sorted order so the result
// is deterministic even though multiple proposals can share a winning key.
func (r *Register) Resolve(prio PriorityLookup) Proposal {
	peers := sortedPeers(r.Proposals)
	if len(peers) == 0 {
		return Proposal{Tombstone: true}
	}

	counts := map[string]int{}
	for _, peer := range peers {
		counts[proposalKey(r.Proposals[peer])]++
	}
	total := len(peers)
	for _, peer := range peers {
		p := r.Proposals[peer]
		if counts[proposalKey(p)]*2 > total {
			return p
		}
	}

	best := r.Proposals[peers[0]]
	bestPrio := prio.Lookup(best.Peer)
	for _, peer := range peers[1:] {
		p := r.Proposals[peer]
		if pr := prio.Lookup(p.Peer); pr < bestPrio {
			bestPrio, best = pr, p
		}
	}
	return best
}

// Line pairs a permanent structural identity with its contested content.
type Line struct {
	ID  LineID
	Reg Register
}

// File is a path's existence register plus its ordered, contested lines.
//
// A File is mutated by pkg/session's single Run goroutine (local edits and
// merged remote deltas alike — see Document.ApplyRemote) but read
// concurrently by pkg/network when it gob-encodes a snapshot for a newly
// joining peer, on network's own goroutine. mu guards every mutable field
// below against that specific read/write race; GobEncode/GobDecode hold it
// too, so wire serialization can't observe a half-written state.
type File struct {
	mu sync.RWMutex

	Path string
	Reg  Register // existence register, keyed by path (not by LineID)
	// Lines is keyed by LineID.key() rather than LineID itself: FracIndex
	// is a slice, so LineID isn't a valid (comparable) Go map key.
	Lines map[string]*Line
	Order []LineID // cache, sorted by Pos; rebuilt on structural change

	Version uint64 // bumped on every register mutation in this file,
	// reset to 0 when pkg/jjrepo confirms a commit (Document.ResetVersion)

	// DeleteObservedAt is the Version a pending delete proposal last saw.
	// Exported (unlike the README's lowercase sketch) so it survives gob
	// encoding across pkg/network — an unexported field would silently
	// vanish on the wire and break the delete-vs-edit check on the
	// receiving peer.
	DeleteObservedAt uint64
}

func NewFile(path string) *File {
	return &File{Path: path, Reg: newRegister(), Lines: map[string]*Line{}}
}

// fileWire is File's plain-data shape for gob (de)serialization. File
// itself carries a mutex, which gob can't walk via reflection, and a
// custom GobEncode/GobDecode lets serialization hold mu instead of racing
// concurrent in-process mutation (see File's doc comment).
type fileWire struct {
	Path             string
	Reg              Register
	Lines            map[string]*Line
	Order            []LineID
	Version          uint64
	DeleteObservedAt uint64
}

func (f *File) GobEncode() ([]byte, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var buf bytes.Buffer
	w := fileWire{f.Path, f.Reg, f.Lines, f.Order, f.Version, f.DeleteObservedAt}
	if err := gob.NewEncoder(&buf).Encode(w); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *File) GobDecode(data []byte) error {
	var w fileWire
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&w); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Path, f.Reg, f.Lines, f.Order = w.Path, w.Reg, w.Lines, w.Order
	f.Version, f.DeleteObservedAt = w.Version, w.DeleteObservedAt
	return nil
}

// rebuildOrderLocked assumes the caller already holds mu for writing.
func (f *File) rebuildOrderLocked() {
	ids := make([]LineID, 0, len(f.Lines))
	for _, line := range f.Lines {
		ids = append(ids, line.ID)
	}
	sort.Slice(ids, func(i, j int) bool { return lineIDLess(ids[i], ids[j]) })
	f.Order = ids
}

// InsertLine adds a brand-new line (structural insert) with peer's initial
// proposal for its content.
func (f *File) InsertLine(id LineID, peer PeerID, value string) *Line {
	f.mu.Lock()
	defer f.mu.Unlock()
	line := &Line{ID: id, Reg: newRegister()}
	line.Reg.Proposals[peer] = Proposal{Peer: peer, Value: value, Seq: nextProposalSeq()}
	f.Lines[id.key()] = line
	f.Version++
	f.rebuildOrderLocked()
	return line
}

// ProposeLineEdit records peer's proposed new content for an existing line.
func (f *File) ProposeLineEdit(id LineID, peer PeerID, value string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	line, ok := f.Lines[id.key()]
	if !ok {
		return
	}
	line.Reg.Proposals[peer] = Proposal{Peer: peer, Value: value, Seq: nextProposalSeq()}
	f.Version++
}

// ProposeLineDelete records peer's proposal that a line no longer exists.
func (f *File) ProposeLineDelete(id LineID, peer PeerID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	line, ok := f.Lines[id.key()]
	if !ok {
		return
	}
	line.Reg.Proposals[peer] = Proposal{Peer: peer, Tombstone: true, Seq: nextProposalSeq()}
	f.Version++
}

// ProposeDelete records peer's proposal that the whole file no longer
// exists, as of the file's current Version — read under this call's own
// lock rather than accepting it as a parameter, so callers never need an
// unsynchronized read of f.Version just to pass it in.
func (f *File) ProposeDelete(peer PeerID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Reg.Proposals[peer] = Proposal{Peer: peer, Tombstone: true, Version: f.Version, Seq: nextProposalSeq()}
	f.DeleteObservedAt = f.Version
	f.Version++
}

// RefreshExistence re-asserts that peer still considers the file to exist,
// as of the file's current Version. pkg/session calls this alongside any
// line-level change so File.ResolveExistence's delete-vs-edit check has a
// non-tombstone proposal newer than a pending delete to find — the delete
// only actually wins if nothing else has touched the file since it was
// proposed.
func (f *File) RefreshExistence(peer PeerID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Reg.Proposals[peer] = Proposal{Peer: peer, Tombstone: false, Version: f.Version, Seq: nextProposalSeq()}
}

// ResolveExistence resolves whether the file exists. A delete only wins if
// no proposal newer than what the delete had observed contradicts it —
// otherwise a delete that merely lost the vote to a stale, already-settled
// edit would incorrectly block ever deleting a file anyone had touched.
func (f *File) ResolveExistence(prio PriorityLookup) Proposal {
	f.mu.RLock()
	defer f.mu.RUnlock()
	winner := f.Reg.Resolve(prio)
	if winner.Tombstone {
		for _, peer := range sortedPeers(f.Reg.Proposals) {
			p := f.Reg.Proposals[peer]
			if !p.Tombstone && p.Version > f.DeleteObservedAt {
				return p
			}
		}
	}
	return winner
}

// Line returns the line with the given identity, if it exists. Exported so
// pkg/session can walk f.Order and propose edits without needing access to
// LineID.key()'s internal encoding.
func (f *File) Line(id LineID) (*Line, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	l, ok := f.Lines[id.key()]
	return l, ok
}

// ResolvedLine is one line's identity paired with its resolved content.
type ResolvedLine struct {
	ID    LineID
	Value string
}

func (f *File) resolvedLinesLocked(prio PriorityLookup) []ResolvedLine {
	lines := make([]ResolvedLine, 0, len(f.Order))
	for _, id := range f.Order {
		line := f.Lines[id.key()]
		if line == nil {
			continue
		}
		resolved := line.Reg.Resolve(prio)
		if resolved.Tombstone {
			continue
		}
		lines = append(lines, ResolvedLine{ID: id, Value: resolved.Value})
	}
	return lines
}

// ResolvedLines resolves every line's register in order, skipping
// tombstoned (deleted) lines.
func (f *File) ResolvedLines(prio PriorityLookup) []ResolvedLine {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.resolvedLinesLocked(prio)
}

// MaterializeContent resolves every line's register in order and joins the
// survivors with "\n" into the file's current whole-file content.
func (f *File) MaterializeContent(prio PriorityLookup) string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	resolved := f.resolvedLinesLocked(prio)
	values := make([]string, len(resolved))
	for i, r := range resolved {
		values[i] = r.Value
	}
	return strings.Join(values, "\n")
}

// Neighbors returns the FracIndex immediately before and after index i in
// f.Order (Begin/End sentinels at the boundaries), for GenerateBetween.
func (f *File) Neighbors(i int) (before, after FracIndex) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	before = Begin
	if i > 0 {
		before = f.Order[i-1].Pos
	}
	after = End
	if i < len(f.Order) {
		after = f.Order[i].Pos
	}
	return before, after
}

// ContributingPeers returns every peer ID with a live proposal anywhere in
// this file — its existence register or any line's register — for
// pkg/session to attribute a commit to the peers who touched it.
func (f *File) ContributingPeers() []PeerID {
	f.mu.RLock()
	defer f.mu.RUnlock()

	set := map[PeerID]bool{}
	for peer := range f.Reg.Proposals {
		set[peer] = true
	}
	for _, id := range f.Order {
		line := f.Lines[id.key()]
		if line == nil {
			continue
		}
		for peer := range line.Reg.Proposals {
			set[peer] = true
		}
	}
	peers := make([]PeerID, 0, len(set))
	for p := range set {
		peers = append(peers, p)
	}
	return peers
}

// Document is the whole synced directory: every known file, keyed by path.
type Document struct {
	mu    sync.RWMutex
	Files map[string]*File
}

func NewDocument() *Document {
	return &Document{Files: map[string]*File{}}
}

// File returns the named file and whether it's known yet.
func (d *Document) File(path string) (*File, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	f, ok := d.Files[path]
	return f, ok
}

// GetOrCreateFile returns the named file, creating an empty one if absent.
func (d *Document) GetOrCreateFile(path string) *File {
	d.mu.Lock()
	defer d.mu.Unlock()
	f, ok := d.Files[path]
	if !ok {
		f = NewFile(path)
		d.Files[path] = f
	}
	return f
}

// Paths returns every known file path.
func (d *Document) Paths() []string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	paths := make([]string, 0, len(d.Files))
	for p := range d.Files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return paths
}

// ResetVersion zeroes a file's Version (and delete-observation marker) once
// pkg/session confirms pkg/jjrepo has committed the round — everyone's
// converged and there's nothing left to disambiguate for that file until
// the next round. Registers themselves are left as-is: Resolve recomputes
// from scratch every time, so stale proposals are harmless, just not
// pruned (acceptable growth for a POC's lifetime).
func (d *Document) ResetVersion(path string) {
	d.mu.RLock()
	f, ok := d.Files[path]
	d.mu.RUnlock()
	if !ok {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.Version = 0
	f.DeleteObservedAt = 0
}

// ApplyRemote merges a remote peer's view of one file into this Document —
// a proposal-wise union per register, never a blind overwrite, so a delta
// that arrives out of order still just adds its proposals. Per-peer
// conflicts within a register are resolved by keeping whichever proposal
// has the higher Seq, so a stale duplicate can't regress a newer one.
func (d *Document) ApplyRemote(remote *File) {
	d.mu.Lock()
	local, ok := d.Files[remote.Path]
	if !ok {
		local = NewFile(remote.Path)
		d.Files[remote.Path] = local
	}
	d.mu.Unlock()

	// remote was just gob-decoded exclusively for this call — nothing else
	// holds a reference to it, so reading its fields needs no lock. local
	// is reachable from d.Files and may be concurrently gob-encoded by
	// pkg/network (see File's doc comment), so its mutation is locked.
	local.mu.Lock()
	defer local.mu.Unlock()

	mergeRegister(&local.Reg, &remote.Reg)
	for key, rline := range remote.Lines {
		lline, ok := local.Lines[key]
		if !ok {
			lline = &Line{ID: rline.ID, Reg: newRegister()}
			local.Lines[key] = lline
		}
		mergeRegister(&lline.Reg, &rline.Reg)
	}

	if remote.Version > local.Version {
		local.Version = remote.Version
	}
	if remote.DeleteObservedAt > local.DeleteObservedAt {
		local.DeleteObservedAt = remote.DeleteObservedAt
	}
	local.rebuildOrderLocked()
}

func mergeRegister(local, remote *Register) {
	for peer, p := range remote.Proposals {
		existing, ok := local.Proposals[peer]
		if !ok || p.Seq >= existing.Seq {
			local.Proposals[peer] = p
		}
	}
}
