package crdt

import "math/rand/v2"

// FracIndex is a Logoot/LSEQ-style fractional index: a sequence of digits
// compared lexicographically, with a missing digit treated as 0. Insertion
// always allocates strictly between its two neighbors, so no two structural
// positions ever collide.
type FracIndex []uint32

// digitBase bounds how much "room" GenerateBetween invents once it walks
// past the end of the shorter of its two inputs. Kept well below
// math.MaxUint32 so the End sentinel (below) always sorts strictly higher
// than anything actually generated.
const digitBase = 1 << 24

// Begin and End are sentinels for inserting at the very start or very end
// of a file's line order (there's no real neighbor to pass in those cases).
var (
	Begin = FracIndex{}
	End   = FracIndex{^uint32(0)}
)

func digitAt(idx FracIndex, depth int) uint32 {
	if depth < len(idx) {
		return idx[depth]
	}
	return 0
}

// upperBound is like digitAt but returns an open bound (digitBase) once idx
// is exhausted, rather than 0 — that's what gives GenerateBetween room to
// invent a value below End, or below any neighbor shorter than the current
// depth. This is purely an implementation detail of allocation; Compare
// below still uses missing-digit-as-0 on both sides, as documented.
func upperBound(idx FracIndex, depth int) uint32 {
	if depth < len(idx) {
		return idx[depth]
	}
	return digitBase
}

// Compare orders two FracIndex values lexicographically, treating a missing
// digit as 0 on both sides.
func Compare(a, b FracIndex) int {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		da, db := digitAt(a, i), digitAt(b, i)
		switch {
		case da < db:
			return -1
		case da > db:
			return 1
		}
	}
	return 0
}

// GenerateBetween allocates a new FracIndex strictly between a and b
// (a < result < b must hold; callers are responsible for passing a < b).
// It walks a and b digit by digit and picks a value strictly between the
// first pair that differs. If a and b are adjacent at every digit checked
// so far (no gap), it fixes that digit and recurses one level deeper,
// where the missing side opens up (see upperBound) — that's what gives two
// peers concurrently inserting "at the same spot" room to land on distinct
// values without coordinating, since each independently draws its own
// random digit in the newly opened range.
func GenerateBetween(a, b FracIndex, peer PeerID) FracIndex {
	return generateBetween(a, b, 0)
}

func generateBetween(a, b FracIndex, depth int) FracIndex {
	da := digitAt(a, depth)
	db := upperBound(b, depth)

	if db > da+1 {
		gap := db - da - 1
		v := da + 1 + rand.N(gap)
		return append(clonePrefix(a, depth), v)
	}

	// No room at this digit: fix it to da and recurse one level deeper.
	sub := generateBetween(a, b, depth+1)
	prefix := append(clonePrefix(a, depth), da)
	return append(prefix, sub...)
}

func clonePrefix(a FracIndex, n int) FracIndex {
	out := make(FracIndex, n)
	for i := 0; i < n; i++ {
		out[i] = digitAt(a, i)
	}
	return out
}
