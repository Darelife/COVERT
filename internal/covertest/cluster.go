// Package covertest is a multi-peer test harness: it drives real
// session.Session instances (real temp dirs, real jj repos, real loopback
// TCP) so higher-level tests can assert on actual convergence instead of
// hand-rolling the plumbing in every test.
package covertest

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/darelife/covert/pkg/session"
	"github.com/stretchr/testify/require"
)

const joinSettleDelay = 150 * time.Millisecond

// Cluster is n real covert peers wired into one mesh: peer 0 is the
// founder, every other peer joined through it.
type Cluster struct {
	t       *testing.T
	peers   []*session.Session
	dirs    []string
	cancels []context.CancelFunc
	wg      sync.WaitGroup
}

// NewCluster starts n peers (n >= 1): peer 0 as founder, the rest joining
// sequentially through it, each on its own temp dir and loopback port. The
// cluster is torn down automatically via t.Cleanup: every peer's Run loop
// is stopped and confirmed exited *before* its directory is removed.
//
// Directories are deliberately NOT created via t.TempDir(): its own
// cleanup is registered the moment it's called, and t.Cleanup runs LIFO —
// since peer directories are created one at a time as the cluster grows,
// a plain t.TempDir() would remove a peer's directory before this
// cluster's single, ordered cleanup gets a chance to stop that peer's
// session first, and a session mid-commit into an already-deleted
// directory is exactly the kind of flake this harness exists to avoid.
func NewCluster(t *testing.T, n int) *Cluster {
	t.Helper()
	require.GreaterOrEqual(t, n, 1)

	c := &Cluster{t: t}
	t.Cleanup(c.shutdownAndClean)

	founderDir := mustMkdirTemp(t)
	founder, err := session.New(founderDir, session.AsFounder("127.0.0.1:0"))
	require.NoError(t, err)
	c.addPeer(founder, founderDir)

	for i := 1; i < n; i++ {
		dir := mustMkdirTemp(t)
		joiner, err := session.New(dir, session.JoinVia(founder.ListenAddr(), "127.0.0.1:0"))
		require.NoError(t, err)
		c.addPeer(joiner, dir)

		// Priority assignment during join has no global consensus (a
		// documented POC limitation in pkg/network/pkg/priority): two
		// peers joining at the exact same instant could race for the same
		// number. Sequential joins with a short settle gap between them
		// sidesteps that race in test code, rather than needing the
		// implementation to solve global consensus for a POC.
		time.Sleep(joinSettleDelay)
	}

	return c
}

func mustMkdirTemp(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "covertest-*")
	require.NoError(t, err)
	return dir
}

func (c *Cluster) addPeer(s *session.Session, dir string) {
	c.peers = append(c.peers, nil)
	c.dirs = append(c.dirs, dir)
	c.cancels = append(c.cancels, nil)
	c.runPeer(len(c.peers)-1, s)
}

func (c *Cluster) runPeer(i int, s *session.Session) {
	ctx, cancel := context.WithCancel(context.Background())
	c.peers[i] = s
	c.cancels[i] = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := s.Run(ctx); err != nil && ctx.Err() == nil {
			c.t.Logf("covertest: peer session.Run exited unexpectedly: %v", err)
		}
	}()
}

// Join adds a new peer to the cluster through peer 0, on a fresh temp dir.
// Returns the new peer's index.
func (c *Cluster) Join() int {
	c.t.Helper()
	dir := mustMkdirTemp(c.t)
	s, err := session.New(dir, session.JoinVia(c.peers[0].ListenAddr(), "127.0.0.1:0"))
	require.NoError(c.t, err)
	c.addPeer(s, dir)
	return len(c.peers) - 1
}

// Leave shuts down peer i's Run loop (simulating a disconnect) without
// touching its directory, so it can Rejoin later.
func (c *Cluster) Leave(i int) {
	c.t.Helper()
	c.cancels[i]()
}

// Rejoin restarts peer i in its existing directory, joining back through
// peer 0. Identity persists across the restart (same directory), so it
// resumes as the same peer but is assigned a strictly worse (demoted)
// priority number, per pkg/priority's rejoin rule.
func (c *Cluster) Rejoin(i int) {
	c.t.Helper()
	s, err := session.New(c.dirs[i], session.JoinVia(c.peers[0].ListenAddr(), "127.0.0.1:0"))
	require.NoError(c.t, err)
	c.runPeer(i, s)
}

// NumPeers returns how many peers are in the cluster.
func (c *Cluster) NumPeers() int { return len(c.peers) }

// Dir returns peer i's working directory.
func (c *Cluster) Dir(i int) string { return c.dirs[i] }

// Session returns peer i's underlying Session, for tests that need direct
// access (e.g. its assigned priority or peer ID).
func (c *Cluster) Session(i int) *session.Session { return c.peers[i] }

// WriteFile writes content to relPath under peer i's working directory,
// exactly as if a user (or their editor) had saved the file.
func (c *Cluster) WriteFile(i int, relPath, content string) {
	c.t.Helper()
	full := filepath.Join(c.dirs[i], relPath)
	require.NoError(c.t, os.MkdirAll(filepath.Dir(full), 0o755))
	require.NoError(c.t, os.WriteFile(full, []byte(content), 0o644))
}

// DeleteFile removes relPath from peer i's working directory.
func (c *Cluster) DeleteFile(i int, relPath string) {
	c.t.Helper()
	require.NoError(c.t, os.Remove(filepath.Join(c.dirs[i], relPath)))
}

// WaitConverged polls until every peer's working tree (excluding VCS/session
// bookkeeping directories) is byte-identical, or fails the test after
// timeout.
func (c *Cluster) WaitConverged(timeout time.Duration) {
	c.t.Helper()
	ok := false
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok, _ = c.converged(); ok {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, diff := c.converged()
	c.t.Fatalf("peers did not converge to identical file trees within %s: %s", timeout, diff)
}

// converged reports whether all peers' trees match, and if not, a
// human-readable description of the first mismatch found (for diagnostics
// on timeout).
func (c *Cluster) converged() (bool, string) {
	var referenceTree map[string]string
	var referenceDir string
	for _, dir := range c.dirs {
		tree, err := snapshotTree(dir)
		if err != nil {
			return false, err.Error()
		}
		if referenceTree == nil {
			referenceTree, referenceDir = tree, dir
			continue
		}
		if !reflect.DeepEqual(referenceTree, tree) {
			return false, diffTrees(referenceDir, referenceTree, dir, tree)
		}
	}
	return true, ""
}

func diffTrees(dirA string, a map[string]string, dirB string, b map[string]string) string {
	for path, va := range a {
		if vb, ok := b[path]; !ok {
			return path + " exists in " + dirA + " but not " + dirB
		} else if va != vb {
			return path + " differs: " + dirA + "=" + va + " " + dirB + "=" + vb
		}
	}
	for path := range b {
		if _, ok := a[path]; !ok {
			return path + " exists in " + dirB + " but not " + dirA
		}
	}
	return "unknown mismatch"
}

func snapshotTree(dir string) (map[string]string, error) {
	out := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, relErr := filepath.Rel(dir, path)
		if relErr != nil {
			return relErr
		}
		if rel == "." {
			return nil
		}
		if strings.HasPrefix(rel, ".git") || strings.HasPrefix(rel, ".jj") || strings.HasPrefix(rel, ".covert") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = string(b)
		return nil
	})
	return out, err
}

// Shutdown cancels every peer's Run loop and waits for each to fully
// return. Safe to call multiple times, and safe to call before the
// t.Cleanup-registered teardown runs (which also removes directories).
func (c *Cluster) Shutdown() {
	for _, cancel := range c.cancels {
		cancel()
	}
	c.wg.Wait()
}

// shutdownAndClean stops every peer and confirms its Run loop has fully
// returned before removing any directory — see the ordering note on
// NewCluster for why this can't just be two separately t.Cleanup-registered
// steps.
func (c *Cluster) shutdownAndClean() {
	c.Shutdown()
	for _, dir := range c.dirs {
		os.RemoveAll(dir)
	}
}
