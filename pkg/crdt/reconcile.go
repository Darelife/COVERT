package crdt

// Op is a single unit of CRDT mutation, exchanged over the network and fed
// into Document.Apply on every peer (including the one that generated it).
type Op struct {
	File     string
	ID       LineID
	Proposal Proposal
}

// ReconcileLocal diffs the document's currently materialized content against
// newLines (freshly read off disk) and returns the Ops needed to bring the
// CRDT up to date with that edit. Callers must Apply() the returned ops
// locally and broadcast them to peers.
func (d *Document) ReconcileLocal(file string, newLines []string, peer string, priorities map[string]int, clock *Clock) []Op {
	old := d.Materialize(priorities)
	oldTexts := make([]string, len(old))
	for i, l := range old {
		oldTexts[i] = l.Content
	}

	script := diffLines(oldTexts, newLines)

	var ops []Op
	oldIdx := 0
	var prev LineID
	havePrev := false

	leftPos := func() string {
		if havePrev {
			return prev.Pos
		}
		return ""
	}
	rightPos := func() string {
		if oldIdx < len(old) {
			return old[oldIdx].ID.Pos
		}
		return ""
	}

	for _, op := range script {
		switch op.kind {
		case editEqual:
			prev = old[oldIdx].ID
			havePrev = true
			oldIdx++
		case editReplace:
			target := old[oldIdx]
			ops = append(ops, Op{
				File: file,
				ID:   target.ID,
				Proposal: Proposal{
					Peer:    peer,
					Seq:     clock.Next(),
					Content: op.text,
				},
			})
			prev = target.ID
			havePrev = true
			oldIdx++
		case editDelete:
			target := old[oldIdx]
			ops = append(ops, Op{
				File: file,
				ID:   target.ID,
				Proposal: Proposal{
					Peer:    peer,
					Seq:     clock.Next(),
					Deleted: true,
				},
			})
			prev = target.ID
			havePrev = true
			oldIdx++
		case editInsert:
			newID := LineID{
				Pos:  keyBetween(leftPos(), rightPos()),
				Peer: peer,
				Seq:  clock.Next(),
			}
			ops = append(ops, Op{
				File: file,
				ID:   newID,
				Proposal: Proposal{
					Peer:    peer,
					Seq:     clock.Next(),
					Content: op.text,
				},
			})
			prev = newID
			havePrev = true
		}
	}
	return ops
}
