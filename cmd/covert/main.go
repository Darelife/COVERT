// Command covert is the CLI: init a new session over a directory, or join
// an existing one through a known peer's address.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/darelife/covert/pkg/session"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "init":
		cmdInit(os.Args[2:])
	case "join":
		cmdJoin(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: covert init [--listen addr] <dir>")
	fmt.Fprintln(os.Stderr, "       covert join [--listen addr] <peer-addr> [dir]")
}

func cmdInit(args []string) {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "listen address")
	fs.Parse(args)
	dir := fs.Arg(0)
	if dir == "" {
		log.Fatal("usage: covert init [--listen addr] <dir>")
	}

	sess, err := session.New(dir, session.AsFounder(*listen))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("listening on", sess.ListenAddr())
	log.Fatal(sess.Run(rootContext()))
}

func cmdJoin(args []string) {
	fs := flag.NewFlagSet("join", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:0", "listen address")
	fs.Parse(args)
	peerAddr := fs.Arg(0)
	dir := fs.Arg(1)
	if dir == "" {
		dir = "."
	}
	if peerAddr == "" {
		log.Fatal("usage: covert join [--listen addr] <peer-addr> [dir]")
	}

	sess, err := session.New(dir, session.JoinVia(peerAddr, *listen))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("listening on", sess.ListenAddr())
	log.Fatal(sess.Run(rootContext()))
}

// rootContext is cancelled on SIGINT/SIGTERM so Run shuts down cleanly.
func rootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()
	return ctx
}
