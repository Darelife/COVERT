// Package watch turns filesystem changes under a directory into debounced
// whole-file-content callbacks, recursively, so the caller can diff them into
// CRDT ops.
package watch

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// ChangeHandler is called with a file's full new content, split into lines,
// on every settled change. A removed file is reported with lines == nil.
type ChangeHandler func(relPath string, lines []string)

// IgnoreFunc reports whether a path (relative to the watch root) should be
// skipped entirely (e.g. VCS internals).
type IgnoreFunc func(relPath string) bool

const debounceWindow = 150 * time.Millisecond

type Watcher struct {
	root    string
	handler ChangeHandler
	ignore  IgnoreFunc
	fsw     *fsnotify.Watcher

	mu      sync.Mutex
	timers  map[string]*time.Timer
	writing map[string]bool // paths we're currently writing ourselves; suppress the resulting event
}

func New(root string, handler ChangeHandler, ignore IgnoreFunc) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &Watcher{
		root:    root,
		handler: handler,
		ignore:  ignore,
		fsw:     fsw,
		timers:  make(map[string]*time.Timer),
		writing: make(map[string]bool),
	}, nil
}

// Start walks the tree, subscribes every directory, and begins delivering
// change events. It blocks the goroutine it's run on, so call with `go`.
func (w *Watcher) Start() error {
	if err := w.addTree(w.root); err != nil {
		return err
	}
	for event := range w.fsw.Events {
		w.handleEvent(event)
	}
	return nil
}

func (w *Watcher) Close() error {
	return w.fsw.Close()
}

// Suppress marks a path as being written by us (e.g. materializing a
// CRDT-resolved version), so the resulting fsnotify event is dropped instead
// of being fed back in as a new "local" edit.
func (w *Watcher) Suppress(relPath string) {
	w.mu.Lock()
	w.writing[relPath] = true
	w.mu.Unlock()
}

func (w *Watcher) unsuppress(relPath string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writing[relPath] {
		delete(w.writing, relPath)
		return true
	}
	return false
}

func (w *Watcher) addTree(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(w.root, path)
		rel = filepath.ToSlash(rel)
		if rel != "." && w.ignore != nil && w.ignore(rel) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return w.fsw.Add(path)
		}
		w.scheduleRead(rel)
		return nil
	})
}

func (w *Watcher) handleEvent(event fsnotify.Event) {
	rel, err := filepath.Rel(w.root, event.Name)
	if err != nil {
		return
	}
	rel = filepath.ToSlash(rel)
	if w.ignore != nil && w.ignore(rel) {
		return
	}

	if event.Op&fsnotify.Create != 0 {
		if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
			w.addTree(event.Name)
			return
		}
	}

	if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Remove|fsnotify.Rename) != 0 {
		w.scheduleRead(rel)
	}
}

func (w *Watcher) scheduleRead(rel string) {
	w.mu.Lock()
	if t, ok := w.timers[rel]; ok {
		t.Stop()
	}
	w.timers[rel] = time.AfterFunc(debounceWindow, func() { w.settle(rel) })
	w.mu.Unlock()
}

func (w *Watcher) settle(rel string) {
	w.mu.Lock()
	delete(w.timers, rel)
	w.mu.Unlock()

	if w.unsuppress(rel) {
		return
	}

	full := filepath.Join(w.root, filepath.FromSlash(rel))
	data, err := os.ReadFile(full)
	if err != nil {
		if os.IsNotExist(err) {
			w.handler(rel, nil)
		}
		return
	}
	if looksBinary(data) {
		return
	}
	w.handler(rel, splitLines(data))
}

func splitLines(data []byte) []string {
	text := strings.TrimSuffix(string(data), "\n")
	if text == "" {
		return []string{}
	}
	return strings.Split(text, "\n")
}

func looksBinary(data []byte) bool {
	n := len(data)
	if n > 8192 {
		n = 8192
	}
	for i := 0; i < n; i++ {
		if data[i] == 0 {
			return true
		}
	}
	return false
}
