package session

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/darelife/covert/pkg/watch"
	"github.com/stretchr/testify/require"
)

func fileEventFor(t *testing.T, path, content string) watch.FileEvent {
	t.Helper()
	return watch.FileEvent{Path: path, Content: []byte(content)}
}

func requireJJ(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("jj"); err != nil {
		t.Skip("jj binary not found in PATH")
	}
}

func TestNewAsFounderSetsUpIdentityAndRepo(t *testing.T) {
	requireJJ(t)
	dir := t.TempDir()

	s, err := New(dir, AsFounder("127.0.0.1:0"))
	require.NoError(t, err)
	defer s.mesh.Close()
	defer s.watcher.Close()

	require.NotEmpty(t, s.Self())
	require.Equal(t, 1, s.prio.Lookup(s.Self()))
	require.DirExists(t, filepath.Join(dir, ".jj"))
	require.NotEmpty(t, s.ListenAddr())

	// A restart in the same directory should resume as the same peer.
	s2, err := New(dir, AsFounder("127.0.0.1:0"))
	require.NoError(t, err)
	defer s2.mesh.Close()
	defer s2.watcher.Close()
	require.Equal(t, s.Self(), s2.Self())
}

// TestCommitDebounceCoalescesRapidChanges drives applyLocalEvent directly
// (bypassing the real filesystem watcher) to verify multiple rapid changes
// within the debounce window collapse into a single jj commit, not one per
// change.
func TestCommitDebounceCoalescesRapidChanges(t *testing.T) {
	requireJJ(t)
	dir := t.TempDir()
	s, err := New(dir, AsFounder("127.0.0.1:0"))
	require.NoError(t, err)
	defer s.mesh.Close()
	defer s.watcher.Close()

	path := filepath.Join(dir, "file.txt")
	for _, content := range []string{"v1", "v1v2", "v1v2v3"} {
		require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
		s.applyLocalEvent(fileEventFor(t, path, content))
	}

	countSyncCommits := func() int {
		n := 0
		for _, d := range jjLogDescriptions(t, dir) {
			if len(d) >= 5 && d[:5] == "sync:" {
				n++
			}
		}
		return n
	}

	require.Eventually(t, func() bool {
		return countSyncCommits() >= 1
	}, 2*time.Second, 20*time.Millisecond, "the debounced commit round should eventually settle")

	require.Equal(t, 1, countSyncCommits(), "rapid changes within the debounce window must collapse into one commit")
}

func jjLogDescriptions(t *testing.T, dir string) []string {
	t.Helper()
	cmd := exec.Command("jj", "log", "--no-graph", "-T", `description ++ "\x00"`)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "jj log failed: %s", out)
	var descs []string
	start := 0
	for i, b := range out {
		if b == 0 {
			descs = append(descs, string(out[start:i]))
			start = i + 1
		}
	}
	return descs
}
