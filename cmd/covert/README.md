# cmd/covert

The CLI, built on [`pkg/session`](../../pkg/session/README.md).

## Subcommands

- `covert init <dir>` — start a session over a directory; the caller becomes
  peer priority 1 (the founder). Prints the listen address others can join
  with.
- `covert join <peer-addr> [dir]` — join an existing session through a known
  peer's address, syncing into `dir` (defaults to the current directory).

## Flag parsing

Flags must come before positional arguments (standard Go `flag` parsing):
`covert join --listen 127.0.0.1:9000 <peer-addr> [dir]`.

## Implementation

```go
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
    log.Fatal(sess.Run(context.Background()))
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
    log.Fatal(sess.Run(context.Background()))
}
```

`session.AsFounder` and `session.JoinVia` are the two `session.Option`
constructors that decide, inside `session.New`, whether to call
`priority.Table.AssignFounder` or dial out and go through
[`pkg/network`](../../pkg/network/README.md#join-flow-concrete-exchange)'s
join exchange — this package makes no `pkg/network` or `pkg/priority` calls
of its own, it only selects which `pkg/session` constructor path to take.
