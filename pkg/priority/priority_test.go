package priority

import (
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAssignFounderClaimsOne(t *testing.T) {
	tbl := New()
	tbl.AssignFounder("founder")
	require.Equal(t, 1, tbl.Lookup("founder"))
}

func TestAssignOrdering(t *testing.T) {
	tbl := New()
	tbl.AssignFounder("founder")

	n1 := tbl.Assign("alice")
	n2 := tbl.Assign("bob")

	require.Equal(t, 2, n1)
	require.Equal(t, 3, n2)
	require.Equal(t, 2, tbl.Lookup("alice"))
	require.Equal(t, 3, tbl.Lookup("bob"))
}

func TestRejoinAlwaysDemotes(t *testing.T) {
	tbl := New()
	tbl.AssignFounder("founder")
	first := tbl.Assign("alice")
	require.Equal(t, 2, first)

	// alice leaves and rejoins: gets a strictly worse number, no special
	// "already known" path.
	second := tbl.Assign("alice")
	require.Greater(t, second, first)
	require.Equal(t, second, tbl.Lookup("alice"))
}

func TestSetAdvancesNextWhenAhead(t *testing.T) {
	tbl := New()
	tbl.Set("remote-peer", 5)
	require.Equal(t, 5, tbl.Lookup("remote-peer"))

	// next assignment must not collide with the learned number.
	n := tbl.Assign("newcomer")
	require.Greater(t, n, 5)
}

func TestLookupUnknownPeerSortsLast(t *testing.T) {
	tbl := New()
	tbl.AssignFounder("founder")
	require.Equal(t, math.MaxInt, tbl.Lookup("stranger"))
}

func TestLoadOrCreateIdentityPersists(t *testing.T) {
	dir := t.TempDir()

	id1, err := LoadOrCreateIdentity(dir)
	require.NoError(t, err)
	require.NotEmpty(t, id1)

	// Simulate a process restart in the same directory: must resume as the
	// same peer, not generate a fresh identity.
	id2, err := LoadOrCreateIdentity(dir)
	require.NoError(t, err)
	require.Equal(t, id1, id2)

	b, err := os.ReadFile(filepath.Join(dir, identityFile))
	require.NoError(t, err)
	require.Equal(t, string(id1), string(b))
}

func TestLoadOrCreateIdentityDifferentDirsDiffer(t *testing.T) {
	id1, err := LoadOrCreateIdentity(t.TempDir())
	require.NoError(t, err)
	id2, err := LoadOrCreateIdentity(t.TempDir())
	require.NoError(t, err)
	require.NotEqual(t, id1, id2)
}
