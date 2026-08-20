// Package watch turns raw OS filesystem events into debounced
// whole-file-content callbacks for pkg/session to feed into pkg/crdt.
package watch

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// FileEvent is a settled, debounced change to one path.
type FileEvent struct {
	Path    string
	Content []byte // nil when Deleted
	Deleted bool
}

// Watcher watches a directory tree and emits one debounced FileEvent per
// path no matter how many raw OS events (write/create/remove) fired for it
// during the debounce window. Raw OS rename events are normalized here into
// a delete-old-path + create-new-path pair before anything downstream sees
// them — fsnotify on Linux already surfaces a rename as two independent
// Remove/Create events for paths inside the watched tree, so no explicit
// pairing logic is needed; a Rename op is just treated as Remove and a
// Create op is just treated as Create.
type Watcher struct {
	root     string
	fsw      *fsnotify.Watcher
	debounce time.Duration

	mu     sync.Mutex
	timers map[string]*time.Timer

	out    chan FileEvent
	errs   chan error
	done   chan struct{}
	closed sync.Once
}

// New starts watching every directory under root (recursively). debounce is
// how long to wait after the last raw event for a path before emitting.
func New(root string, debounce time.Duration) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	w := &Watcher{
		root:     root,
		fsw:      fsw,
		debounce: debounce,
		timers:   map[string]*time.Timer{},
		out:      make(chan FileEvent, 64),
		errs:     make(chan error, 8),
		done:     make(chan struct{}),
	}

	if err := w.addTree(root); err != nil {
		fsw.Close()
		return nil, err
	}

	go w.loop()
	return w, nil
}

func (w *Watcher) addTree(root string) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if strings.HasPrefix(d.Name(), ".") && path != root {
				return filepath.SkipDir // skip .git, .jj, .covert, etc.
			}
			return w.fsw.Add(path)
		}
		return nil
	})
}

// Events returns the channel of settled, debounced file events.
func (w *Watcher) Events() <-chan FileEvent { return w.out }

// Errors returns the channel of underlying fsnotify errors.
func (w *Watcher) Errors() <-chan error { return w.errs }

func (w *Watcher) loop() {
	for {
		select {
		case ev, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			w.handle(ev)
		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			select {
			case w.errs <- err:
			default:
			}
		case <-w.done:
			return
		}
	}
}

func (w *Watcher) handle(ev fsnotify.Event) {
	// A newly created directory needs to be watched too, so nested files
	// under it also surface events.
	if ev.Op&fsnotify.Create != 0 {
		if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
			w.fsw.Add(ev.Name)
			return
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if t, ok := w.timers[ev.Name]; ok {
		t.Stop()
	}
	deleted := ev.Op&(fsnotify.Remove|fsnotify.Rename) != 0
	w.timers[ev.Name] = time.AfterFunc(w.debounce, func() {
		w.emit(ev.Name, deleted)
	})
}

func (w *Watcher) emit(path string, deleted bool) {
	// A fast remove-then-recreate (common with editors that save via
	// temp-file-then-rename) collapses into a single Create rather than a
	// spurious delete-then-create pair, because we check the current disk
	// state at emit time rather than trusting the op that triggered the
	// timer.
	content, err := os.ReadFile(path)
	if err != nil {
		if !deleted {
			return // file vanished before the debounce fired; nothing to emit
		}
		w.send(FileEvent{Path: path, Deleted: true})
		return
	}
	w.send(FileEvent{Path: path, Content: content})
}

func (w *Watcher) send(ev FileEvent) {
	select {
	case w.out <- ev:
	case <-w.done:
	}
}

// Close stops the watcher and releases underlying OS resources.
func (w *Watcher) Close() error {
	w.closed.Do(func() { close(w.done) })
	return w.fsw.Close()
}
