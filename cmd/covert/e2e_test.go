package main

import (
	"bufio"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func requireJJBin(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("jj"); err != nil {
		t.Skip("jj binary not found in PATH")
	}
}

// buildCovertBinary compiles the real CLI once per test so the e2e test
// exercises the actual init/join subcommands, not a stand-in.
func buildCovertBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "covert")
	cmd := exec.Command("go", "build", "-o", bin, ".")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "go build failed: %s", out)
	return bin
}

type peerProc struct {
	cmd  *exec.Cmd
	addr string
}

// startPeer launches `covert <args...>` as a real subprocess and waits for
// it to print its listen address (cmd/covert's `fmt.Println("listening
// on", ...)` on both the init and join paths).
func startPeer(t *testing.T, bin string, args ...string) *peerProc {
	t.Helper()
	cmd := exec.Command(bin, args...)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	addrCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if rest, ok := strings.CutPrefix(line, "listening on "); ok {
				select {
				case addrCh <- rest:
				default:
				}
			}
		}
	}()

	select {
	case addr := <-addrCh:
		return &peerProc{cmd: cmd, addr: addr}
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		t.Fatal("timed out waiting for peer to report its listen address")
		return nil
	}
}

func (p *peerProc) stop(t *testing.T) {
	t.Helper()
	if p.cmd.Process == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		p.cmd.Process.Kill()
		<-done
	}
}

func waitForFileContent(t *testing.T, path, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(path); err == nil {
			last = string(b)
			if last == want {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to contain %q, last saw %q", path, want, last)
}

// TestE2ETwoRealProcessesConverge is the slow, real-subprocess end of the
// test pyramid: an actual `covert init` and `covert join` process, real
// filesystem writes, real debounce timers, real `jj` commits. Everything
// below this layer (crdt, priority, watch, network, jjrepo, session unit +
// in-process integration tests) runs in milliseconds to a few seconds;
// this one takes real wall-clock time for the watch+commit debounce, so
// it's gated out of the fast default `go test ./... -short` path.
func TestE2ETwoRealProcessesConverge(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e: real subprocesses + debounce timers, skipped in -short mode")
	}
	requireJJBin(t)

	bin := buildCovertBinary(t)
	dir1 := t.TempDir()
	dir2 := t.TempDir()

	founder := startPeer(t, bin, "init", "--listen", "127.0.0.1:0", dir1)
	defer founder.stop(t)

	joiner := startPeer(t, bin, "join", "--listen", "127.0.0.1:0", founder.addr, dir2)
	defer joiner.stop(t)

	path1 := filepath.Join(dir1, "hello.txt")
	require.NoError(t, os.WriteFile(path1, []byte("hello from the real CLI"), 0o644))

	path2 := filepath.Join(dir2, "hello.txt")
	waitForFileContent(t, path2, "hello from the real CLI", 15*time.Second)

	// Confirm the sync round actually landed as a jj commit, not just a
	// filesystem write racing the assertion.
	logCmd := exec.Command("jj", "log", "--no-graph", "-T", `description ++ "\n"`)
	logCmd.Dir = dir2
	out, err := logCmd.CombinedOutput()
	require.NoError(t, err, "jj log failed: %s", out)
	require.Contains(t, string(out), "sync: hello.txt")
}
