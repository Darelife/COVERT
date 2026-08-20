package session

import "github.com/darelife/covert/pkg/crdt"

// diffKind classifies one step of turning old line values into new ones.
type diffKind byte

const (
	diffKeep diffKind = iota
	diffChange
	diffDelete
	diffInsert
)

type diffOp struct {
	kind   diffKind
	oldIdx int    // valid for keep/change/delete: index into the old slice
	value  string // valid for change/insert
}

// diffLines computes a line-level edit script from old to new via LCS,
// then greedily pairs up adjacent non-matched old/new lines into "change"
// ops rather than a separate delete+insert — the same shape a human diff
// tool produces for "line 5 changed" instead of "line 5 removed, new line
// 5 added".
func diffLines(old, new []string) []diffOp {
	oldLCS, newLCS := lcsIndices(old, new)

	var ops []diffOp
	oi, ni, li := 0, 0, 0
	for oi < len(old) || ni < len(new) {
		if li < len(oldLCS) && oi == oldLCS[li] && ni == newLCS[li] {
			ops = append(ops, diffOp{kind: diffKeep, oldIdx: oi})
			oi++
			ni++
			li++
			continue
		}
		oldIsNextMatch := li < len(oldLCS) && oi == oldLCS[li]
		newIsNextMatch := li < len(newLCS) && ni == newLCS[li]

		switch {
		case oi < len(old) && !oldIsNextMatch && ni < len(new) && !newIsNextMatch:
			ops = append(ops, diffOp{kind: diffChange, oldIdx: oi, value: new[ni]})
			oi++
			ni++
		case oi < len(old) && !oldIsNextMatch:
			ops = append(ops, diffOp{kind: diffDelete, oldIdx: oi})
			oi++
		case ni < len(new) && !newIsNextMatch:
			ops = append(ops, diffOp{kind: diffInsert, value: new[ni]})
			ni++
		default:
			// Both sides are sitting at their next LCS match but the
			// indices didn't line up in the fast-path check above — can't
			// happen for a correct LCS, but avoid an infinite loop if it
			// somehow does.
			oi++
			ni++
		}
	}
	return ops
}

// lcsIndices returns the indices into a and b (in lockstep) that make up
// their longest common subsequence, via the standard O(n*m) DP table —
// files in a POC are small, so a full Myers implementation buys nothing.
func lcsIndices(a, b []string) (aIdx, bIdx []int) {
	n, m := len(a), len(b)
	dp := make([][]int, n+1)
	for i := range dp {
		dp[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			switch {
			case a[i] == b[j]:
				dp[i][j] = dp[i+1][j+1] + 1
			case dp[i+1][j] >= dp[i][j+1]:
				dp[i][j] = dp[i+1][j]
			default:
				dp[i][j] = dp[i][j+1]
			}
		}
	}

	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			aIdx = append(aIdx, i)
			bIdx = append(bIdx, j)
			i++
			j++
		case dp[i+1][j] >= dp[i][j+1]:
			i++
		default:
			j++
		}
	}
	return aIdx, bIdx
}

// applyDiffToFile turns a diff script into crdt proposals: keeps are
// no-ops, changes/deletes act on the existing line at oldResolved[oldIdx],
// and inserts allocate a fresh position between whatever currently
// surrounds them (the last processed survivor, and the next one still to
// come) via crdt.GenerateBetween.
func applyDiffToFile(f *crdt.File, oldResolved []crdt.ResolvedLine, ops []diffOp, self crdt.PeerID) {
	// nextSurvivingPos[i] is the FracIndex of the next kept/changed line at
	// or after op index i, or crdt.End if there isn't one — precomputed so
	// each insert knows its upper bound regardless of intervening deletes.
	nextSurvivingPos := make([]crdt.FracIndex, len(ops)+1)
	nextSurvivingPos[len(ops)] = crdt.End
	for i := len(ops) - 1; i >= 0; i-- {
		if ops[i].kind == diffKeep || ops[i].kind == diffChange {
			nextSurvivingPos[i] = oldResolved[ops[i].oldIdx].ID.Pos
		} else {
			nextSurvivingPos[i] = nextSurvivingPos[i+1]
		}
	}

	before := crdt.Begin
	for i, op := range ops {
		switch op.kind {
		case diffKeep:
			before = oldResolved[op.oldIdx].ID.Pos
		case diffChange:
			f.ProposeLineEdit(oldResolved[op.oldIdx].ID, self, op.value)
			before = oldResolved[op.oldIdx].ID.Pos
		case diffDelete:
			f.ProposeLineDelete(oldResolved[op.oldIdx].ID, self)
		case diffInsert:
			after := nextSurvivingPos[i+1]
			pos := crdt.GenerateBetween(before, after, self)
			f.InsertLine(crdt.LineID{Pos: pos, Peer: self}, self, op.value)
			before = pos
		}
	}
}
