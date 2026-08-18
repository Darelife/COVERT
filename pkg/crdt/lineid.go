package crdt

import (
	"fmt"
	"strings"
	"sync"
)

// alphabet defines the base used for fractional-index position keys.
// Ordering of generated keys follows plain byte/string comparison of this alphabet's order.
const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

const base = len(alphabet)

func digitVal(c byte) int {
	return strings.IndexByte(alphabet, c)
}

// keyBetween returns a string that sorts strictly between a and b (a < b).
// "" as a means "the very beginning", "" as b means "the very end".
// Two concurrent calls with the same (a, b) may return the same string; LineID
// breaks the tie deterministically via Peer/Seq, so this never causes data loss.
func keyBetween(a, b string) string {
	var result []byte
	i := 0
	for {
		da := 0
		if i < len(a) {
			da = digitVal(a[i])
		}
		db := base
		if i < len(b) {
			db = digitVal(b[i])
		}
		if db-da > 1 {
			mid := da + (db-da)/2
			result = append(result, alphabet[mid])
			return string(result)
		}
		result = append(result, alphabet[da])
		i++
		if i > 500 {
			// Pathological case (thousands of same-spot inserts); bail out safely.
			result = append(result, alphabet[base/2])
			return string(result)
		}
	}
}

// LineID identifies a line/element in the document CRDT. Pos is the fractional
// index that determines document order; Peer+Seq guarantee global uniqueness
// and give a deterministic tiebreak when two peers generate the same Pos
// concurrently (e.g. both inserting after the same line at the same time).
type LineID struct {
	Pos  string
	Peer string
	Seq  uint64
}

func (id LineID) String() string {
	return fmt.Sprintf("%s\x00%s\x00%020d", id.Pos, id.Peer, id.Seq)
}

// Less defines the total order lines are materialized in.
func (id LineID) Less(other LineID) bool {
	if id.Pos != other.Pos {
		return id.Pos < other.Pos
	}
	if id.Peer != other.Peer {
		return id.Peer < other.Peer
	}
	return id.Seq < other.Seq
}

func (id LineID) IsZero() bool {
	return id.Pos == "" && id.Peer == "" && id.Seq == 0
}

// Clock is a per-peer monotonic counter used both for LineID.Seq (uniqueness)
// and Proposal.Seq (freshness). Reusing one counter for both keeps ordering
// simple: everything this peer ever produced has a total order.
type Clock struct {
	mu  sync.Mutex
	cur uint64
}

func (c *Clock) Next() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cur++
	return c.cur
}

// Observe advances the clock past a value seen from a remote op, so locally
// generated ops always sort after anything we've received (Lamport-style).
func (c *Clock) Observe(seen uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seen > c.cur {
		c.cur = seen
	}
}
