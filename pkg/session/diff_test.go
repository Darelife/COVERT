package session

import (
	"testing"

	"github.com/darelife/covert/pkg/crdt"
	"github.com/stretchr/testify/require"
)

type fakePriority map[crdt.PeerID]int

func (f fakePriority) Lookup(p crdt.PeerID) int {
	if n, ok := f[p]; ok {
		return n
	}
	return 1 << 30
}

// roundTrip drives a File through applyDiffToFile as if `self` had edited
// it from oldContent to newContent, then returns what MaterializeContent
// produces — i.e. the observable result pkg/session's caller sees.
func roundTrip(t *testing.T, oldContent, newContent string, self crdt.PeerID) string {
	t.Helper()
	f := crdt.NewFile("doc.txt")
	prio := fakePriority{self: 1}

	// Seed the "old" state via one initial diff from empty.
	seed := diffLines(nil, splitLines(oldContent))
	applyDiffToFile(f, nil, seed, self)

	oldResolved := f.ResolvedLines(prio)
	oldValues := make([]string, len(oldResolved))
	for i, r := range oldResolved {
		oldValues[i] = r.Value
	}
	ops := diffLines(oldValues, splitLines(newContent))
	applyDiffToFile(f, oldResolved, ops, self)

	return f.MaterializeContent(prio)
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

func TestDiffRoundTripUnchanged(t *testing.T) {
	got := roundTrip(t, "a\nb\nc", "a\nb\nc", "alice")
	require.Equal(t, "a\nb\nc", got)
}

func TestDiffRoundTripAppend(t *testing.T) {
	got := roundTrip(t, "a\nb", "a\nb\nc", "alice")
	require.Equal(t, "a\nb\nc", got)
}

func TestDiffRoundTripPrepend(t *testing.T) {
	got := roundTrip(t, "b\nc", "a\nb\nc", "alice")
	require.Equal(t, "a\nb\nc", got)
}

func TestDiffRoundTripMiddleInsert(t *testing.T) {
	got := roundTrip(t, "a\nc", "a\nb\nc", "alice")
	require.Equal(t, "a\nb\nc", got)
}

func TestDiffRoundTripDelete(t *testing.T) {
	got := roundTrip(t, "a\nb\nc", "a\nc", "alice")
	require.Equal(t, "a\nc", got)
}

func TestDiffRoundTripChange(t *testing.T) {
	got := roundTrip(t, "a\nb\nc", "a\nx\nc", "alice")
	require.Equal(t, "a\nx\nc", got)
}

func TestDiffChangeDoesNotForkTheLine(t *testing.T) {
	f := crdt.NewFile("doc.txt")
	seed := diffLines(nil, []string{"a", "b", "c"})
	applyDiffToFile(f, nil, seed, "alice")
	require.Len(t, f.Lines, 3)

	oldResolved := f.ResolvedLines(fakePriority{"alice": 1})
	ops := diffLines([]string{"a", "b", "c"}, []string{"a", "x", "c"})
	applyDiffToFile(f, oldResolved, ops, "alice")

	// A same-position content change must reuse the existing line's
	// identity (edit its register), not delete+insert a new one.
	require.Len(t, f.Lines, 3, "changing a line's content must not change the line count")
}

func TestDiffEmptyToContent(t *testing.T) {
	got := roundTrip(t, "", "hello", "alice")
	require.Equal(t, "hello", got)
}

func TestDiffContentToEmpty(t *testing.T) {
	got := roundTrip(t, "hello", "", "alice")
	require.Equal(t, "", got)
}
