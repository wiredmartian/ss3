// Package watcher normalizes fsnotify filesystem events into event.Event.
package watcher

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/ss3/internal/event"
)

// writeDebounce is how long consecutive Write events on the same path are
// coalesced into a single F:WRITE notification.
const writeDebounce = 100 * time.Millisecond

type Watcher struct {
	dir string
	fsw *fsnotify.Watcher

	mu     sync.Mutex
	timers map[string]*time.Timer
	// wg tracks debounce-timer callbacks (see debounceWrite) that have
	// fired and are running (or about to run) w.emit. run() waits on it
	// before closing events/errs, so a callback can never send on a
	// channel that has already been closed.
	wg     sync.WaitGroup
	events chan event.Event
	errs   chan error
}

func New(dir string) (*Watcher, error) {
	fsw, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("create fsnotify watcher: %w", err)
	}
	if err := fsw.Add(dir); err != nil {
		fsw.Close()
		return nil, fmt.Errorf("watch dir %q: %w", dir, err)
	}
	return &Watcher{
		dir:    dir,
		fsw:    fsw,
		timers: make(map[string]*time.Timer),
		events: make(chan event.Event),
		errs:   make(chan error),
	}, nil
}

// Watch starts the watch loop and returns channels for normalized events and
// errors. Both channels are closed once ctx is done and the watcher has shut
// down.
func (w *Watcher) Watch(ctx context.Context) (<-chan event.Event, <-chan error) {
	go w.run(ctx)
	return w.events, w.errs
}

func (w *Watcher) run(ctx context.Context) {
	defer w.fsw.Close()
	defer close(w.events)
	defer close(w.errs)

	for {
		select {
		case <-ctx.Done():
			w.mu.Lock()
			for _, t := range w.timers {
				if t.Stop() {
					// Canceled before it fired, so its callback (and the
					// wg.Done it would have called) will never run: account
					// for that here instead.
					w.wg.Done()
				}
			}
			w.mu.Unlock()
			// Wait for any callback that had already fired (Stop returned
			// false above, or fired before we even got here) to finish its
			// own w.emit call before we close w.events/w.errs below.
			w.wg.Wait()
			return

		case fsEv, ok := <-w.fsw.Events:
			if !ok {
				return
			}
			w.handle(ctx, fsEv)

		case err, ok := <-w.fsw.Errors:
			if !ok {
				return
			}
			select {
			case w.errs <- err:
			case <-ctx.Done():
				return
			}
		}
	}
}

// opTypes lists every fsnotify op we track alongside its event.Type, in a
// fixed order so a combined bitmask emits deterministically.
var opTypes = []struct {
	op fsnotify.Op
	t  event.Type
}{
	{fsnotify.Create, event.Create},
	{fsnotify.Remove, event.Remove},
	{fsnotify.Rename, event.Rename},
	{fsnotify.Write, event.Write},
	{fsnotify.Chmod, event.Chmod},
}

func (w *Watcher) handle(ctx context.Context, fsEv fsnotify.Event) {
	for _, ot := range opTypes {
		if !fsEv.Op.Has(ot.op) {
			continue
		}
		if ot.op == fsnotify.Write {
			w.debounceWrite(ctx, fsEv.Name)
			continue
		}
		w.emit(ctx, ot.t, fsEv.Name)
	}
}

// debounceWrite coalesces bursts of Write events on the same path into a
// single F:WRITE emission, fired writeDebounce after the last event.
func (w *Watcher) debounceWrite(ctx context.Context, path string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if t, ok := w.timers[path]; ok {
		if t.Stop() {
			// Canceled before it fired: its callback (and the wg.Done it
			// would have called) will never run, so balance the WaitGroup
			// here instead.
			w.wg.Done()
		}
		// If Stop returns false, the old timer already fired (or is in the
		// middle of firing); its own callback is responsible for deleting
		// its map entry (guarded below by identity, not just path) and for
		// calling wg.Done() once it finishes.
	}

	w.wg.Add(1)
	var t *time.Timer
	t = time.AfterFunc(writeDebounce, func() {
		defer w.wg.Done()
		w.mu.Lock()
		// Only remove our own entry: if a newer timer for path has since
		// replaced us in the map, leave it alone.
		if w.timers[path] == t {
			delete(w.timers, path)
		}
		w.mu.Unlock()
		w.emit(ctx, event.Write, path)
	})
	w.timers[path] = t
}

func (w *Watcher) emit(ctx context.Context, t event.Type, path string) {
	var size int64
	if t == event.Create || t == event.Write {
		if info, err := os.Stat(path); err == nil {
			size = info.Size()
		}
	}
	ev := event.New(t, path, size)
	select {
	case w.events <- ev:
	case <-ctx.Done():
	}
}
