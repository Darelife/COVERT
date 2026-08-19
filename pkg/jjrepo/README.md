# pkg/jjrepo

A thin exec wrapper around the `jj` CLI.

## Live commits

The working directory is a colocated git+jj repo (`jj git init`). Every time
a sync round changes any file's resolved content, this package writes the
resolved content to disk and runs `jj commit -m "sync: <files> (by
<peers>)"` — jj's own working-copy auto-snapshot picks up the change, so this
both finalizes the description and opens a fresh commit on top.

Multiple rapid edits are debounced (by [`pkg/watch`](../watch/README.md) and
[`pkg/session`](../session/README.md)) into one commit rather than one per
keystroke.

## Commit as the reset trigger

A successful commit here is the signal [`pkg/session`](../session/README.md)
uses to reset [`pkg/crdt`](../crdt/README.md)'s per-file version counters —
once a round is committed, everyone's converged and there's nothing left to
disambiguate for that file until the next round.

## Implementation

```go
type Repo struct{ dir string }

// Init runs `jj git init` in dir (colocated git+jj), creating dir if needed.
func Init(dir string) (*Repo, error)

// Change describes one file's resolved outcome for this round.
type Change struct {
    Path    string
    Content []byte // nil + Deleted for a resolved-delete
    Deleted bool
}

func (r *Repo) Commit(changes []Change, peers []priority.PeerID) error {
    var touched []string
    for _, c := range changes {
        full := filepath.Join(r.dir, c.Path)
        if c.Deleted {
            if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
                return err
            }
        } else {
            if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
                return err
            }
            if err := os.WriteFile(full, c.Content, 0o644); err != nil {
                return err
            }
        }
        touched = append(touched, c.Path)
    }
    msg := fmt.Sprintf("sync: %s (by %s)",
        strings.Join(touched, ", "), strings.Join(peerNames(peers), ", "))
    cmd := exec.Command("jj", "commit", "-m", msg)
    cmd.Dir = r.dir
    out, err := cmd.CombinedOutput()
    if err != nil && !isNoChangesError(out) {
        return fmt.Errorf("jj commit: %w: %s", err, out)
    }
    return nil // a "no changes to commit" exit is treated as success, not an error
}
```

`jj`'s own working-copy auto-snapshot (triggered on every `jj` invocation,
including this `commit`) is what actually records the file writes/removals
above as the *previous* commit's final state before `-m` opens the next
one — this package never calls `jj diffedit`, `jj describe`, or touches the
git plumbing directly, it only ever shells out to `jj commit`.

`isNoChangesError` matches jj's "Nothing changed" exit message: this can
legitimately happen if a resolved value already matches what's on disk
(e.g. this peer's own proposal won and it never differed locally), and
should not be surfaced as a failure to [`pkg/session`](../session/README.md).
