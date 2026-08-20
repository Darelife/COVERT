package jjrepo

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/darelife/covert/pkg/priority"
	"github.com/stretchr/testify/require"
)

func requireJJ(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("jj"); err != nil {
		t.Skip("jj binary not found in PATH")
	}
}

func jjLog(t *testing.T, dir string) string {
	t.Helper()
	cmd := exec.Command("jj", "log", "--no-graph", "-T", `description ++ "\n"`)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "jj log failed: %s", out)
	return string(out)
}

func TestInitCreatesColocatedRepo(t *testing.T) {
	requireJJ(t)
	dir := filepath.Join(t.TempDir(), "repo")

	repo, err := Init(dir)
	require.NoError(t, err)
	require.Equal(t, dir, repo.Dir())
	require.DirExists(t, filepath.Join(dir, ".git"))
	require.DirExists(t, filepath.Join(dir, ".jj"))
}

func TestCommitWritesFilesAndCreatesCommit(t *testing.T) {
	requireJJ(t)
	dir := t.TempDir()
	repo, err := Init(dir)
	require.NoError(t, err)

	err = repo.Commit([]Change{
		{Path: "hello.txt", Content: []byte("hello world")},
	}, []priority.PeerID{"alice"})
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dir, "hello.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello world", string(content))

	log := jjLog(t, dir)
	require.Contains(t, log, "sync: hello.txt (by alice)")
}

func TestCommitDeletesFile(t *testing.T) {
	requireJJ(t)
	dir := t.TempDir()
	repo, err := Init(dir)
	require.NoError(t, err)

	require.NoError(t, repo.Commit([]Change{
		{Path: "gone.txt", Content: []byte("temporary")},
	}, []priority.PeerID{"alice"}))

	require.NoError(t, repo.Commit([]Change{
		{Path: "gone.txt", Deleted: true},
	}, []priority.PeerID{"bob"}))

	_, err = os.Stat(filepath.Join(dir, "gone.txt"))
	require.True(t, os.IsNotExist(err))
}

func TestCommitCreatesNestedDirs(t *testing.T) {
	requireJJ(t)
	dir := t.TempDir()
	repo, err := Init(dir)
	require.NoError(t, err)

	err = repo.Commit([]Change{
		{Path: "nested/dir/file.txt", Content: []byte("deep")},
	}, []priority.PeerID{"alice"})
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(dir, "nested", "dir", "file.txt"))
	require.NoError(t, err)
	require.Equal(t, "deep", string(content))
}

func TestCommitWithNoChangesDoesNotError(t *testing.T) {
	requireJJ(t)
	dir := t.TempDir()
	repo, err := Init(dir)
	require.NoError(t, err)

	// Empty change set: pkg/session's settleRound relies on this being a
	// harmless no-op, not an error, for rounds with nothing to write.
	require.NoError(t, repo.Commit(nil, nil))
}

func TestCommitMultiplePeersJoinedInMessage(t *testing.T) {
	requireJJ(t)
	dir := t.TempDir()
	repo, err := Init(dir)
	require.NoError(t, err)

	err = repo.Commit([]Change{
		{Path: "a.txt", Content: []byte("a")},
		{Path: "b.txt", Content: []byte("b")},
	}, []priority.PeerID{"alice", "bob"})
	require.NoError(t, err)

	log := jjLog(t, dir)
	require.True(t, strings.Contains(log, "sync: a.txt, b.txt (by alice, bob)"))
}
