# pkg/watch

The filesystem watcher: turns raw OS filesystem events into debounced
whole-file-content callbacks for [`pkg/session`](../session/README.md) to
feed into [`pkg/crdt`](../crdt/README.md).

## Debouncing

Multiple rapid OS-level writes to the same file are debounced into a single
callback carrying the file's current whole content, rather than firing once
per write.

## Rename normalization

Raw OS rename events are normalized here into a delete-old-path +
create-new-path pair before anything downstream sees them.
[`pkg/crdt`](../crdt/README.md) has no rename-specific logic — by the time an
event reaches it, a rename already looks like two ordinary path operations.

## Implementation

Built on `fsnotify`, which on Linux surfaces `inotify`'s `IN_MOVED_FROM` /
`IN_MOVED_TO` pair as two independent `fsnotify.Remove`/`fsnotify.Create`
events (no cookie correlation needed) as long as both the old and new path
are inside the watched tree — which they always are here, since the whole
session directory is watched. That's why "normalization" requires no
matching logic: a `Rename` op is just treated as `Remove`, and a `Create` op
is just treated as `Create`; the pairing falls out of the OS's own event
pair.

```go
type FileEvent struct {
    Path    string
    Content []byte // nil when Deleted
    Deleted bool
}

type Watcher struct {
    fsw      *fsnotify.Watcher
    debounce time.Duration // e.g. 300ms
    mu       sync.Mutex
    timers   map[string]*time.Timer
    out      chan<- FileEvent
}

func (w *Watcher) handle(ev fsnotify.Event) {
    w.mu.Lock()
    defer w.mu.Unlock()
    if t, ok := w.timers[ev.Name]; ok {
        t.Stop()
    }
    w.timers[ev.Name] = time.AfterFunc(w.debounce, func() {
        w.emit(ev.Name, ev.Op&fsnotify.Remove != 0)
    })
}

func (w *Watcher) emit(path string, deleted bool) {
    if deleted {
        w.out <- FileEvent{Path: path, Deleted: true}
        return
    }
    content, err := os.ReadFile(path)
    if err != nil {
        return // file vanished between the debounce firing and the read; a
                // later Remove event (if any) will still fire its own emit
    }
    w.out <- FileEvent{Path: path, Content: content}
}
```

Every raw event for a path — `Write`, `Create`, or `Remove` — resets the
same per-path timer, so N rapid writes plus a final settle produce exactly
one `FileEvent`. `Remove` still goes through the same debounce path (not an
immediate emit) so that a fast remove-then-recreate (common with editors
that save via temp-file-then-rename) collapses into a single `Create`
rather than a spurious delete-then-create pair reaching
[`pkg/session`](../session/README.md).
