// Package jjrepo is a thin exec wrapper around the jj CLI. Every sync round
// that changes a file's resolved content writes the change to disk and runs
// `jj commit`, so the working directory's colocated git+jj history is a
// live log of converged rounds.
package jjrepo

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/darelife/covert/pkg/priority"
)

type Repo struct{ dir string }

// Init runs `jj git init` in dir (colocated git+jj), creating dir if
// needed.
func Init(dir string) (*Repo, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	cmd := exec.Command("jj", "git", "init")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("jj git init: %w: %s", err, out)
	}
	return &Repo{dir: dir}, nil
}

// Open wraps an already-initialized jj repo at dir without re-running init.
func Open(dir string) *Repo { return &Repo{dir: dir} }

func (r *Repo) Dir() string { return r.dir }

// Change describes one file's resolved outcome for this round.
type Change struct {
	Path    string
	Content []byte // nil + Deleted for a resolved-delete
	Deleted bool
}

// Commit writes changes to disk and runs `jj commit`. jj's own
// working-copy auto-snapshot (triggered on every jj invocation, including
// this one) is what actually records the writes/removals below as the
// *previous* commit's final state before -m opens the next one — this
// package never calls jj diffedit, jj describe, or touches git plumbing
// directly.
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

	if len(touched) == 0 {
		return nil
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

func peerNames(peers []priority.PeerID) []string {
	names := make([]string, len(peers))
	for i, p := range peers {
		names[i] = string(p)
	}
	return names
}

// isNoChangesError matches jj's "Nothing changed" exit message: this can
// legitimately happen if a resolved value already matches what's on disk
// (e.g. this peer's own proposal won and it never differed locally), and
// should not be surfaced as a failure to pkg/session.
func isNoChangesError(out []byte) bool {
	return strings.Contains(string(out), "Nothing changed")
}
