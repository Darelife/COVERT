// Package jjrepo drives the jj CLI to turn resolved CRDT state into a live,
// git-backed jj commit history: every converged sync round becomes one commit.
package jjrepo

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type Repo struct {
	Dir string
}

func Open(dir string) *Repo {
	return &Repo{Dir: dir}
}

// EnsureInit creates a git-backed jj repo in Dir if one doesn't already exist.
func (r *Repo) EnsureInit() error {
	if _, err := os.Stat(filepath.Join(r.Dir, ".jj")); err == nil {
		return nil
	}
	return r.run("git", "init")
}

// Commit snapshots the current working copy (jj does this automatically on
// every invocation) and finalizes it as a new commit with the given message,
// leaving a fresh empty commit on top as the new working-copy change.
func (r *Repo) Commit(message string) error {
	return r.run("commit", "-m", message)
}

func (r *Repo) run(args ...string) error {
	cmd := exec.Command("jj", args...)
	cmd.Dir = r.Dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("jj %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}
