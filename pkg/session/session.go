// Package session wires everything else into one daemon over a working
// directory: pkg/watch -> pkg/crdt -> pkg/network -> pkg/jjrepo.
package session

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/darelife/covert/pkg/crdt"
	"github.com/darelife/covert/pkg/jjrepo"
	"github.com/darelife/covert/pkg/network"
	"github.com/darelife/covert/pkg/priority"
	"github.com/darelife/covert/pkg/watch"
)

// watchDebounce and commitDebounce are deliberately separate: watchDebounce
// coalesces OS-level writes into one watch.FileEvent; commitDebounce
// coalesces FileEvents/network deltas into one jj commit.
var (
	watchDebounce  = 150 * time.Millisecond
	commitDebounce = 300 * time.Millisecond
)

type Session struct {
	dir     string
	self    priority.PeerID
	doc     *crdt.Document
	prio    *priority.Table
	mesh    *network.Mesh
	repo    *jjrepo.Repo
	watcher *watch.Watcher

	roundMu     sync.Mutex
	dirty       map[string]bool // touched since the last commit
	commitTimer *time.Timer
	// commitWG tracks a scheduled-but-not-yet-finished commit round: Add(1)
	// happens whenever a timer is armed, Done() either when it's
	// successfully cancelled before firing or when settleRound completes.
	// stopCommitTimer waits on it so shutdown can't race a jj commit that's
	// already in flight against, say, a test harness removing the
	// directory out from under it.
	commitWG sync.WaitGroup
}

// Option finishes configuring a Session's network identity — either as the
// founding peer or by joining an existing one. Exactly one must be passed
// to New.
type Option func(*Session) error

// AsFounder starts the session as the founding peer (priority 1), listening
// on listenAddr for others to join.
func AsFounder(listenAddr string) Option {
	return func(s *Session) error {
		mesh, err := network.NewFounder(s.self, s.prio, s.doc, listenAddr)
		if err != nil {
			return err
		}
		s.mesh = mesh
		return nil
	}
}

// JoinVia joins an existing session through a known peer's address,
// listening on listenAddr for others to (subsequently) join through us.
func JoinVia(peerAddr, listenAddr string) Option {
	return func(s *Session) error {
		mesh, err := network.NewJoiner(s.self, s.prio, s.doc, listenAddr, peerAddr)
		if err != nil {
			return err
		}
		s.mesh = mesh
		return nil
	}
}

// New sets up a session over dir: loads or creates this peer's persistent
// identity, opens (or initializes) the colocated git+jj repo, starts
// watching the directory, then applies opt to establish network identity.
func New(dir string, opt Option) (*Session, error) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(absDir, 0o755); err != nil {
		return nil, err
	}

	selfID, err := priority.LoadOrCreateIdentity(absDir)
	if err != nil {
		return nil, fmt.Errorf("loading identity: %w", err)
	}

	repo, err := openOrInitRepo(absDir)
	if err != nil {
		return nil, fmt.Errorf("opening repo: %w", err)
	}

	watcher, err := watch.New(absDir, watchDebounce)
	if err != nil {
		return nil, fmt.Errorf("starting watcher: %w", err)
	}

	s := &Session{
		dir:     absDir,
		self:    selfID,
		doc:     crdt.NewDocument(),
		prio:    priority.New(),
		repo:    repo,
		watcher: watcher,
		dirty:   map[string]bool{},
	}

	if err := opt(s); err != nil {
		watcher.Close()
		return nil, err
	}
	return s, nil
}

func openOrInitRepo(dir string) (*jjrepo.Repo, error) {
	if _, err := os.Stat(filepath.Join(dir, ".jj")); err == nil {
		return jjrepo.Open(dir), nil
	}
	return jjrepo.Init(dir)
}

func (s *Session) Dir() string           { return s.dir }
func (s *Session) Self() priority.PeerID { return s.self }
func (s *Session) ListenAddr() string    { return s.mesh.ListenAddr() }

// Run drives the session until ctx is cancelled: local filesystem changes
// become crdt proposals broadcast to the mesh, and inbound deltas from
// other peers (already merged into s.doc by pkg/network) re-arm the
// commit debounce.
func (s *Session) Run(ctx context.Context) error {
	defer s.watcher.Close()
	defer s.mesh.Close()
	defer s.stopCommitTimer()

	events := s.watcher.Events()
	inbound := s.mesh.Incoming()
	for {
		select {
		case ev := <-events:
			s.applyLocalEvent(ev)
		case d := <-inbound:
			s.applyRemote(d)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// applyLocalEvent diffs the file's previous known content against the
// new content line-by-line, turns the diff into crdt proposals, broadcasts
// the touched file to the mesh, and arms the commit debounce.
func (s *Session) applyLocalEvent(ev watch.FileEvent) {
	relPath, err := filepath.Rel(s.dir, ev.Path)
	if err != nil {
		return
	}
	relPath = filepath.ToSlash(relPath)

	f := s.doc.GetOrCreateFile(relPath)
	self := crdt.PeerID(s.self)

	if ev.Deleted {
		f.ProposeDelete(self)
	} else {
		oldResolved := f.ResolvedLines(s.prioLookup())
		oldValues := make([]string, len(oldResolved))
		for i, r := range oldResolved {
			oldValues[i] = r.Value
		}
		newValues := strings.Split(string(ev.Content), "\n")

		applyDiffToFile(f, oldResolved, diffLines(oldValues, newValues), self)
		f.RefreshExistence(self)
	}

	s.markDirty(relPath)
	s.mesh.BroadcastDelta([]*crdt.File{f})
	s.armCommitTimer()
}

// applyRemote merges a batch of remote files into s.doc and re-arms the
// commit debounce. The merge deliberately happens here, on Run's single
// select-loop goroutine, rather than on pkg/network's own read/join/dial
// goroutines — s.doc's crdt.File values aren't otherwise synchronized, and
// applyLocalEvent mutates them from this same goroutine, so merging
// anywhere else would race it.
func (s *Session) applyRemote(d network.Delta) {
	for _, f := range d.Files {
		s.doc.ApplyRemote(f)
		s.markDirty(f.Path)
	}
	s.armCommitTimer()
}

func (s *Session) markDirty(path string) {
	s.roundMu.Lock()
	defer s.roundMu.Unlock()
	s.dirty[path] = true
}

func (s *Session) armCommitTimer() {
	s.roundMu.Lock()
	defer s.roundMu.Unlock()
	if s.commitTimer != nil && s.commitTimer.Stop() {
		// Successfully cancelled before firing: settleRound will never run
		// for it, so release the Add(1) made when it was scheduled.
		s.commitWG.Done()
	}
	s.commitWG.Add(1)
	s.commitTimer = time.AfterFunc(commitDebounce, s.runSettleRound)
}

func (s *Session) runSettleRound() {
	defer s.commitWG.Done()
	s.settleRound()
}

// stopCommitTimer cancels any pending commit round and, if one was already
// in flight (past cancellation, mid jj-commit), waits for it to finish —
// so a caller that tears down the working directory right after Run
// returns can't race an in-progress commit into it.
func (s *Session) stopCommitTimer() {
	s.roundMu.Lock()
	if s.commitTimer != nil && s.commitTimer.Stop() {
		s.commitWG.Done()
	}
	s.roundMu.Unlock()
	s.commitWG.Wait()
}

func (s *Session) settleRound() {
	s.roundMu.Lock()
	files := make([]string, 0, len(s.dirty))
	for p := range s.dirty {
		files = append(files, p)
	}
	s.dirty = map[string]bool{}
	s.roundMu.Unlock()

	if len(files) == 0 {
		return
	}

	prio := s.prioLookup()
	peerSet := map[priority.PeerID]bool{}
	changes := make([]jjrepo.Change, 0, len(files))
	for _, p := range files {
		f, ok := s.doc.File(p)
		if !ok {
			continue
		}
		resolved := f.ResolveExistence(prio)
		if resolved.Tombstone {
			changes = append(changes, jjrepo.Change{Path: p, Deleted: true})
		} else {
			changes = append(changes, jjrepo.Change{Path: p, Content: []byte(f.MaterializeContent(prio))})
		}
		for _, peer := range contributingPeers(f) {
			peerSet[peer] = true
		}
	}

	peers := make([]priority.PeerID, 0, len(peerSet))
	for p := range peerSet {
		peers = append(peers, p)
	}
	sort.Slice(peers, func(i, j int) bool { return peers[i] < peers[j] })

	if err := s.repo.Commit(changes, peers); err != nil {
		log.Printf("session: commit failed, will retry next round: %v", err)
		return // files stay out of s.dirty; next incoming event re-dirties them
	}
	for _, p := range files {
		s.doc.ResetVersion(p)
	}
}

// contributingPeers runs on settleRound's own timer goroutine, not Run's
// select loop, so it must go through crdt.File's locked accessor rather
// than reading f.Order/f.Reg/f.Lines directly.
func contributingPeers(f *crdt.File) []priority.PeerID {
	crdtPeers := f.ContributingPeers()
	peers := make([]priority.PeerID, len(crdtPeers))
	for i, p := range crdtPeers {
		peers[i] = priority.PeerID(p)
	}
	return peers
}

// prioAdapter bridges pkg/priority.Table (keyed by priority.PeerID) to
// pkg/crdt.PriorityLookup (keyed by crdt.PeerID) — the two types are kept
// distinct so pkg/crdt has no dependency on pkg/priority, per that
// package's README.
type prioAdapter struct{ t *priority.Table }

func (p prioAdapter) Lookup(id crdt.PeerID) int { return p.t.Lookup(priority.PeerID(id)) }

func (s *Session) prioLookup() crdt.PriorityLookup { return prioAdapter{s.prio} }
