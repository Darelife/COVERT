package crdt

// editKind labels one step of a line-level edit script.
type editKind byte

const (
	editEqual editKind = iota
	editDelete
	editInsert
	editReplace // in-place content change of the aligned old line (preserves its LineID)
)

type editOp struct {
	kind editKind
	text string // insert/replace: new text. delete/equal: unused (old side identified by position).
}

// diffLines computes a minimal edit script turning old into new, at line
// granularity, via a classic O(n*m) LCS, then pairs up adjacent delete+insert
// runs into editReplace steps. Pairing matters for CRDT semantics: a plain
// delete+insert would let a changed line silently reappear as a brand new
// LineID, so two peers editing the same line concurrently would never
// contend for it - they'd just both "win" as two separate lines. Replacing
// in place keeps the original LineID, so concurrent edits of one line
// actually compete as proposals and go through vote/priority resolution.
func diffLines(old, new []string) []editOp {
	return pairReplacements(lcsScript(old, new))
}

func lcsScript(old, new []string) []editOp {
	n, m := len(old), len(new)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if old[i] == new[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else if lcs[i+1][j] >= lcs[i][j+1] {
				lcs[i][j] = lcs[i+1][j]
			} else {
				lcs[i][j] = lcs[i][j+1]
			}
		}
	}

	var ops []editOp
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case old[i] == new[j]:
			ops = append(ops, editOp{kind: editEqual})
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			ops = append(ops, editOp{kind: editDelete})
			i++
		default:
			ops = append(ops, editOp{kind: editInsert, text: new[j]})
			j++
		}
	}
	for ; i < n; i++ {
		ops = append(ops, editOp{kind: editDelete})
	}
	for ; j < m; j++ {
		ops = append(ops, editOp{kind: editInsert, text: new[j]})
	}
	return ops
}

// pairReplacements walks contiguous delete/insert runs (a "change block")
// and pairs them off index-wise into editReplace steps, leaving any surplus
// on the delete or insert side as-is.
func pairReplacements(ops []editOp) []editOp {
	out := make([]editOp, 0, len(ops))
	i := 0
	for i < len(ops) {
		if ops[i].kind == editEqual {
			out = append(out, ops[i])
			i++
			continue
		}

		start := i
		var dels, inss int
		for i < len(ops) && (ops[i].kind == editDelete || ops[i].kind == editInsert) {
			if ops[i].kind == editDelete {
				dels++
			} else {
				inss++
			}
			i++
		}
		block := ops[start:i]

		paired := dels
		if inss < paired {
			paired = inss
		}

		insIdx := 0
		for k := 0; k < paired; k++ {
			for insIdx < len(block) && block[insIdx].kind != editInsert {
				insIdx++
			}
			out = append(out, editOp{kind: editReplace, text: block[insIdx].text})
			insIdx++
		}
		remainingDels := dels - paired
		for k := 0; k < remainingDels; k++ {
			out = append(out, editOp{kind: editDelete})
		}
		for ; insIdx < len(block); insIdx++ {
			if block[insIdx].kind == editInsert {
				out = append(out, block[insIdx])
			}
		}
	}
	return out
}
