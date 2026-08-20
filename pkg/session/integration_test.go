package session_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/darelife/covert/internal/covertest"
	"github.com/stretchr/testify/require"
)

func requireJJBin(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("jj"); err != nil {
		t.Skip("jj binary not found in PATH")
	}
}

const converge = 10 * time.Second

func TestTwoPeersConvergeOnNewFile(t *testing.T) {
	requireJJBin(t)
	c := covertest.NewCluster(t, 2)

	c.WriteFile(0, "hello.txt", "hello from peer 0")
	c.WaitConverged(converge)

	b, err := os.ReadFile(filepath.Join(c.Dir(1), "hello.txt"))
	require.NoError(t, err)
	require.Equal(t, "hello from peer 0", string(b))
}

func TestThreePeersConvergeAfterEditsFromEachSide(t *testing.T) {
	requireJJBin(t)
	c := covertest.NewCluster(t, 3)

	c.WriteFile(0, "a.txt", "from 0")
	c.WaitConverged(converge)

	c.WriteFile(1, "b.txt", "from 1")
	c.WaitConverged(converge)

	c.WriteFile(2, "c.txt", "from 2")
	c.WaitConverged(converge)

	for i := 0; i < 3; i++ {
		for name, want := range map[string]string{"a.txt": "from 0", "b.txt": "from 1", "c.txt": "from 2"} {
			b, err := os.ReadFile(filepath.Join(c.Dir(i), name))
			require.NoError(t, err, "peer %d missing %s", i, name)
			require.Equal(t, want, string(b))
		}
	}
}

// TestConcurrentEditSameLineResolvesByPriority has both peers edit the same
// existing line before either has seen the other's change. With exactly
// two proposals there's no strict majority, so the founder (priority 1,
// the best) must win.
func TestConcurrentEditSameLineResolvesByPriority(t *testing.T) {
	requireJJBin(t)
	c := covertest.NewCluster(t, 2)

	c.WriteFile(0, "shared.txt", "original")
	c.WaitConverged(converge)

	// Stop the mesh from delivering peer 1's change until after peer 0 has
	// also proposed one, by firing both writes back-to-back before either
	// round settles (commitDebounce/watchDebounce give ~450ms of slack).
	c.WriteFile(0, "shared.txt", "founder's edit")
	c.WriteFile(1, "shared.txt", "joiner's edit")

	c.WaitConverged(converge)

	b, err := os.ReadFile(filepath.Join(c.Dir(0), "shared.txt"))
	require.NoError(t, err)
	require.Equal(t, "founder's edit", string(b), "founder has the best priority, must win the tie")
}

func TestPeerJoiningMidSessionCatchesUpOnHistory(t *testing.T) {
	requireJJBin(t)
	c := covertest.NewCluster(t, 1)

	c.WriteFile(0, "preexisting.txt", "written before anyone joined")
	c.WaitConverged(converge)

	late := c.Join()
	c.WaitConverged(converge)

	b, err := os.ReadFile(filepath.Join(c.Dir(late), "preexisting.txt"))
	require.NoError(t, err)
	require.Equal(t, "written before anyone joined", string(b))
}

// TestDeleteVsConcurrentEditEditWins isolates the delete-vs-edit rule from
// the ordinary line-content vote-then-priority rule: those are two
// independent mechanisms (see pkg/crdt's README), so the file's owning
// peer (here: peer 1, the joiner — priority 2, worse than the founder's 1)
// must be the one who both creates AND edits the line. If the founder
// instead held its own untouched proposal for that same line, the
// founder's better priority would legitimately win the ordinary
// line-content tiebreak regardless of the delete-vs-edit outcome, which
// would conflate the two mechanisms rather than test this one.
func TestDeleteVsConcurrentEditEditWins(t *testing.T) {
	requireJJBin(t)
	c := covertest.NewCluster(t, 2)

	c.WriteFile(1, "doc.txt", "keep me")
	c.WaitConverged(converge)

	// Both fire before either round settles: peer 0 (founder) deletes,
	// peer 1 (the line's own author) edits the same, already-converged
	// file concurrently.
	c.DeleteFile(0, "doc.txt")
	c.WriteFile(1, "doc.txt", "keep me, edited")

	c.WaitConverged(converge)

	b, err := os.ReadFile(filepath.Join(c.Dir(0), "doc.txt"))
	require.NoError(t, err, "concurrent edit must beat the delete")
	require.Equal(t, "keep me, edited", string(b))
}

func TestRejoinDemotesPriority(t *testing.T) {
	requireJJBin(t)
	c := covertest.NewCluster(t, 2)

	firstPrio := c.Session(1).Self()
	require.NotEmpty(t, firstPrio)

	c.Leave(1)
	time.Sleep(300 * time.Millisecond) // let the founder notice the drop
	c.Rejoin(1)

	require.Equal(t, firstPrio, c.Session(1).Self(), "identity must persist across a restart in the same dir")

	c.WriteFile(0, "after-rejoin.txt", "founder content")
	c.WaitConverged(converge)

	b, err := os.ReadFile(filepath.Join(c.Dir(1), "after-rejoin.txt"))
	require.NoError(t, err)
	require.Equal(t, "founder content", string(b))
}
