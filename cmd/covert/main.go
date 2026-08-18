// covert is a P2P live-collaborative-editing daemon: it watches a directory,
// syncs edits with peers via a CRDT, resolves conflicts by majority vote
// (falling back to join-order priority), and streams the converged result
// into a live jj commit history.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"covert/pkg/session"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		cmdInit(os.Args[2:])
	case "join":
		cmdJoin(os.Args[2:])
	case "-h", "--help", "help":
		usage()
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `covert - live P2P collaborative editing with CRDT sync and live jj commits

Usage:
  covert init [--listen host:port] [dir]
      Start a brand new session over dir (default: current directory).
      You become priority 1 (founder). Prints the address others should
      pass to 'covert join'.

  covert join [--listen host:port] <peer-addr> [dir]
      Join an existing session by connecting to any current member's
      address. Priority is assigned by join order; rejoining under a new
      process always gets a fresh, lower priority.

--listen defaults to 127.0.0.1:0 (an OS-assigned free port). Flags must come
before the positional arguments (standard Go flag-parsing behavior).
`)
}

func cmdInit(args []string) {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "address to listen on and advertise to peers")
	fs.Parse(args)

	dir := "."
	if fs.NArg() > 0 {
		dir = fs.Arg(0)
	}

	sess, addr := mustNewSession(dir, *listen)
	sess.InitFounder()
	sess.Start()

	fmt.Printf("covert session started\n  peer id: %s (priority 1, founder)\n  listen:  %s\n  dir:     %s\n\nOthers can join with:\n  covert join %s\n\n", sess.SelfID, addr, sess.Dir, addr)
	runForever()
}

func cmdJoin(args []string) {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "address to listen on and advertise to peers")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "usage: covert join <peer-addr> [dir]")
		os.Exit(1)
	}
	peerAddr := fs.Arg(0)
	dir := "."
	if fs.NArg() > 1 {
		dir = fs.Arg(1)
	}

	sess, addr := mustNewSession(dir, *listen)
	if err := sess.JoinVia(peerAddr); err != nil {
		fatal(err)
	}
	sess.Start()

	pr, _ := sess.Registry.Get(sess.SelfID)
	fmt.Printf("joined session via %s\n  peer id: %s (priority %d)\n  listen:  %s\n  dir:     %s\n\n", peerAddr, sess.SelfID, pr, addr, sess.Dir)
	runForever()
}

func mustNewSession(dir, listen string) (*session.Session, string) {
	absDir, err := filepath.Abs(dir)
	if err != nil {
		fatal(err)
	}
	id, err := session.LoadOrCreateIdentity(absDir)
	if err != nil {
		fatal(err)
	}

	sess, addr, err := session.New(absDir, id, listen)
	if err != nil {
		fatal(err)
	}
	sess.OnPeerUp = func(id string) { fmt.Printf("[peer up]   %s\n", id) }
	sess.OnPeerDown = func(id string) { fmt.Printf("[peer down] %s\n", id) }
	return sess, addr
}

func runForever() {
	fmt.Println("watching for local edits and peer syncs. Ctrl+C to stop.")
	select {}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
