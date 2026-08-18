// Package session wires together the CRDT documents, the P2P network, the
// filesystem watcher, and the jj repo into one live-sync daemon over a
// working directory.
package session

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"covert/pkg/crdt"
	"covert/pkg/jjrepo"
	"covert/pkg/network"
	"covert/pkg/priority"
	"covert/pkg/watch"
)

// commitDebounce batches bursts of edits (local or remote) into one jj
// commit instead of one commit per keystroke/line.
const commitDebounce = 600 * time.Millisecond

type Session struct {
	Dir      string
	SelfID   string
	Registry *priority.Registry
	Node     *network.Node
	Repo     *jjrepo.Repo

	watcher *watch.Watcher
	clock   *crdt.Clock

	// OnPeerUp/OnPeerDown, if set, are called after this session's own
	// bookkeeping (e.g. full-state sync) runs for the event.
	OnPeerUp   func(peerID string)
	OnPeerDown func(peerID string)

	mu            sync.Mutex
	docs          map[string]*crdt.Document
	dirty         map[string]bool
	contributors  map[string]bool
	lastCommitted map[string]string // rel path -> content as of the last jj commit
	commitTimer   *time.Timer
}

// New creates a session rooted at dir, binds the P2P listener at bindAddr
// (port 0 picks a free port), and ensures dir is a git-backed jj repo. It
// does not yet assign a priority or start watching - call InitFounder or
// JoinVia, then Start.
func New(dir, selfID, bindAddr string) (*Session, string, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, "", err
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, "", err
	}

	reg := priority.NewRegistry()
	node := network.NewNode(selfID, reg)
	addr, err := node.Listen(bindAddr)
	if err != nil {
		return nil, "", err
	}

	repo := jjrepo.Open(absDir)
	if err := repo.EnsureInit(); err != nil {
		return nil, "", err
	}

	// Seeding from wall-clock time (instead of starting at 0) means a
	// restarted process's proposals are virtually guaranteed to have a
	// higher Seq than anything it produced in a prior session under the
	// same (persistent) identity, so peers who remember its old high-water
	// mark won't reject its post-rejoin edits as stale.
	clock := &crdt.Clock{}
	clock.Observe(uint64(time.Now().UnixNano()))

	s := &Session{
		Dir:           absDir,
		SelfID:        selfID,
		Registry:      reg,
		Node:          node,
		Repo:          repo,
		clock:         clock,
		docs:          make(map[string]*crdt.Document),
		dirty:         make(map[string]bool),
		contributors:  make(map[string]bool),
		lastCommitted: make(map[string]string),
	}
	node.OnOp = s.handleRemoteOp
	node.OnPeerUp = s.handlePeerUp
	node.OnPeerDown = func(id string) {
		if s.OnPeerDown != nil {
			s.OnPeerDown(id)
		}
	}

	w, err := watch.New(absDir, s.handleLocalChange, ignorePath)
	if err != nil {
		return nil, "", err
	}
	s.watcher = w

	return s, addr, nil
}

// InitFounder claims priority 1. Call this instead of JoinVia when starting
// a brand new session.
func (s *Session) InitFounder() {
	s.Registry.InitFounder(s.SelfID)
}

// JoinVia connects to an existing session member and receives a priority
// assignment (always a fresh, worse-than-current one if we've been here
// before under this same peer ID).
func (s *Session) JoinVia(addr string) error {
	return s.Node.Join(addr)
}

// Start begins watching the working directory for local edits. Runs in the
// background; call blocks nothing.
func (s *Session) Start() {
	go s.watcher.Start()
}

func ignorePath(rel string) bool {
	for _, part := range strings.Split(rel, "/") {
		if part == ".jj" || part == ".git" || part == identityDir {
			return true
		}
	}
	return false
}

// handlePeerUp replicates this session's full CRDT state (across all files)
// to a newly connected peer, so joining mid-session (or reconnecting after a
// mesh-repair dial) still yields the peer the complete document history, not
// just future edits. It runs symmetrically on both ends of every new link,
// so whichever side has pre-existing content propagates it to the other.
func (s *Session) handlePeerUp(peerID string) {
	s.mu.Lock()
	docs := make(map[string]*crdt.Document, len(s.docs))
	for rel, d := range s.docs {
		docs[rel] = d
	}
	s.mu.Unlock()

	for rel, doc := range docs {
		doc.ForEach(func(id crdt.LineID, p crdt.Proposal) {
			s.Node.SendOp(peerID, crdt.Op{File: rel, ID: id, Proposal: p})
		})
	}

	if s.OnPeerUp != nil {
		s.OnPeerUp(peerID)
	}
}

func (s *Session) getDoc(rel string) *crdt.Document {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.docs[rel]
	if !ok {
		d = crdt.NewDocument()
		s.docs[rel] = d
	}
	return d
}

// handleLocalChange is the watcher callback: a file changed on disk because
// the local user edited it.
func (s *Session) handleLocalChange(rel string, lines []string) {
	if lines == nil {
		lines = []string{}
	}
	doc := s.getDoc(rel)
	priorities := s.Registry.Snapshot()
	ops := doc.ReconcileLocal(rel, lines, s.SelfID, priorities, s.clock)
	if len(ops) == 0 {
		return
	}
	for _, op := range ops {
		doc.Apply(op.ID, op.Proposal)
		s.Node.Broadcast(op)
	}
	s.markDirty(rel, s.SelfID)
}

// handleRemoteOp is the network callback: a peer sent us a CRDT op.
func (s *Session) handleRemoteOp(op crdt.Op) {
	doc := s.getDoc(op.File)
	if !doc.Apply(op.ID, op.Proposal) {
		return
	}
	s.markDirty(op.File, op.Proposal.Peer)
}

func (s *Session) markDirty(rel, peer string) {
	s.mu.Lock()
	s.dirty[rel] = true
	s.contributors[peer] = true
	if s.commitTimer != nil {
		s.commitTimer.Stop()
	}
	s.commitTimer = time.AfterFunc(commitDebounce, s.flush)
	s.mu.Unlock()
}

// flush materializes every dirty document, writes any resolved content that
// isn't already on disk, and folds every file whose resolved state actually
// moved forward since the last commit into a single live jj commit.
//
// Whether a file needs a *commit* (its resolution changed since we last
// committed it) is tracked independently of whether it needs a *disk write*
// (its resolution differs from what's currently on disk): those often
// diverge - e.g. when the local peer's own edit is the one that wins a vote,
// disk already matches the resolution, but it's still a new state that must
// be recorded in jj.
func (s *Session) flush() {
	s.mu.Lock()
	dirty := s.dirty
	s.dirty = make(map[string]bool)
	contributors := s.contributors
	s.contributors = make(map[string]bool)
	s.mu.Unlock()

	if len(dirty) == 0 {
		return
	}

	priorities := s.Registry.Snapshot()
	var changedFiles []string
	for rel := range dirty {
		doc := s.getDoc(rel)
		content := renderContent(doc.Materialize(priorities))

		s.mu.Lock()
		last, seen := s.lastCommitted[rel]
		s.mu.Unlock()
		if seen && last == content {
			continue
		}

		s.syncDisk(rel, content)

		s.mu.Lock()
		s.lastCommitted[rel] = content
		s.mu.Unlock()
		changedFiles = append(changedFiles, rel)
	}
	if len(changedFiles) == 0 {
		return
	}
	sort.Strings(changedFiles)

	peers := make([]string, 0, len(contributors))
	for p := range contributors {
		peers = append(peers, p)
	}
	sort.Strings(peers)

	msg := fmt.Sprintf("sync: %s (by %s)", strings.Join(changedFiles, ", "), strings.Join(peers, ", "))
	if err := s.Repo.Commit(msg); err != nil {
		fmt.Fprintf(os.Stderr, "jj commit failed: %v\n", err)
	}
}

// syncDisk writes content to rel's file, or removes it if content is empty,
// but only if it differs from what's already there.
func (s *Session) syncDisk(rel, content string) {
	full := filepath.Join(s.Dir, filepath.FromSlash(rel))

	if content == "" {
		if _, err := os.Stat(full); err != nil {
			return // already absent
		}
		s.watcher.Suppress(rel)
		if err := os.Remove(full); err != nil {
			fmt.Fprintf(os.Stderr, "remove %s: %v\n", full, err)
		}
		return
	}

	if existing, err := os.ReadFile(full); err == nil && string(existing) == content {
		return
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "mkdir %s: %v\n", filepath.Dir(full), err)
		return
	}
	s.watcher.Suppress(rel)
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "write %s: %v\n", full, err)
	}
}

func renderContent(resolved []crdt.ResolvedLine) string {
	if len(resolved) == 0 {
		return ""
	}
	lines := make([]string, len(resolved))
	for i, l := range resolved {
		lines[i] = l.Content
	}
	return strings.Join(lines, "\n") + "\n"
}
