package session

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
)

const identityDir = ".covert"
const identityFile = "identity"

// LoadOrCreateIdentity returns a peer ID that's stable across process
// restarts for this directory. This matters for priority semantics: a peer
// that exits and rejoins should be demoted under its *own* identity (a fresh
// Registry.Assign for the same ID naturally overwrites its old priority, and
// its new proposals naturally supersede its old ones). Without a stable ID,
// a rejoin would instead show up as a brand new stranger, leaving its old
// identity's priority and proposals permanently orphaned in every peer's
// state.
func LoadOrCreateIdentity(dir string) (string, error) {
	path := filepath.Join(dir, identityDir, identityFile)

	if data, err := os.ReadFile(path); err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	}

	id, err := randomID()
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o644); err != nil {
		return "", err
	}
	return id, nil
}

func randomID() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
