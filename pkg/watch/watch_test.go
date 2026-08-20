package watch

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const testDebounce = 50 * time.Millisecond

func waitEvent(t *testing.T, w *Watcher, timeout time.Duration) FileEvent {
	t.Helper()
	select {
	case ev := <-w.Events():
		return ev
	case err := <-w.Errors():
		t.Fatalf("watcher error: %v", err)
	case <-time.After(timeout):
		t.Fatal("timed out waiting for file event")
	}
	return FileEvent{}
}

func requireNoEvent(t *testing.T, w *Watcher, wait time.Duration) {
	t.Helper()
	select {
	case ev := <-w.Events():
		t.Fatalf("unexpected event: %+v", ev)
	case <-time.After(wait):
	}
}

func TestRapidWritesCollapseToOneEvent(t *testing.T) {
	dir := t.TempDir()
	w, err := New(dir, testDebounce)
	require.NoError(t, err)
	defer w.Close()

	path := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("v1"), 0o644))
	time.Sleep(testDebounce / 2)
	require.NoError(t, os.WriteFile(path, []byte("v2"), 0o644))
	time.Sleep(testDebounce / 2)
	require.NoError(t, os.WriteFile(path, []byte("v3"), 0o644))

	ev := waitEvent(t, w, 2*time.Second)
	require.Equal(t, path, ev.Path)
	require.False(t, ev.Deleted)
	require.Equal(t, "v3", string(ev.Content))

	// N rapid writes must produce exactly one FileEvent, not one per write.
	requireNoEvent(t, w, 3*testDebounce)
}

func TestDeleteEmitsDeletedEvent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("v1"), 0o644))

	w, err := New(dir, testDebounce)
	require.NoError(t, err)
	defer w.Close()

	require.NoError(t, os.Remove(path))

	ev := waitEvent(t, w, 2*time.Second)
	require.Equal(t, path, ev.Path)
	require.True(t, ev.Deleted)
	require.Nil(t, ev.Content)
}

func TestRenameSurfacesAsDeleteThenCreate(t *testing.T) {
	dir := t.TempDir()
	oldPath := filepath.Join(dir, "old.txt")
	newPath := filepath.Join(dir, "new.txt")
	require.NoError(t, os.WriteFile(oldPath, []byte("content"), 0o644))

	w, err := New(dir, testDebounce)
	require.NoError(t, err)
	defer w.Close()

	require.NoError(t, os.Rename(oldPath, newPath))

	seen := map[string]FileEvent{}
	for i := 0; i < 2; i++ {
		ev := waitEvent(t, w, 2*time.Second)
		seen[ev.Path] = ev
	}

	oldEv, ok := seen[oldPath]
	require.True(t, ok, "expected a delete event for the old path")
	require.True(t, oldEv.Deleted)

	newEv, ok := seen[newPath]
	require.True(t, ok, "expected a create event for the new path")
	require.False(t, newEv.Deleted)
	require.Equal(t, "content", string(newEv.Content))
}

func TestRemoveThenRecreateCollapsesToCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("v1"), 0o644))

	w, err := New(dir, testDebounce)
	require.NoError(t, err)
	defer w.Close()

	// Editors that save via temp-file-then-rename produce a fast
	// remove-then-recreate on the same path; this must collapse into a
	// single Create, not a spurious delete-then-create pair.
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, []byte("v2"), 0o644))

	ev := waitEvent(t, w, 2*time.Second)
	require.Equal(t, path, ev.Path)
	require.False(t, ev.Deleted)
	require.Equal(t, "v2", string(ev.Content))

	requireNoEvent(t, w, 3*testDebounce)
}
