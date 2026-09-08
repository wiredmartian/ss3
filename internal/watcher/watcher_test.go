package watcher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ss3/internal/event"
)

func TestWatcherEmitsCreateWriteRemove(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.csv")

	w, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, errs := w.Watch(ctx)
	go drainErrors(t, errs)

	// Create and write as separate syscalls (rather than a single os.WriteFile)
	// so the platform watcher (e.g. kqueue on darwin) has a chance to attach
	// to the new file's vnode before the write happens.
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	f.Close()

	waitFor(t, events, event.Create, path)

	f, err = os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.WriteString("hello"); err != nil {
		t.Fatalf("WriteString: %v", err)
	}
	f.Close()

	waitFor(t, events, event.Write, path)

	if err := os.Remove(path); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	waitFor(t, events, event.Remove, path)
}

// waitFor reads events until it sees one of type want for path, ignoring any
// others (e.g. an extra debounced F:WRITE), or fails the test after 2s.
func waitFor(t *testing.T, events <-chan event.Event, want event.Type, path string) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-events:
			if ev.Path != path {
				t.Fatalf("event path = %q, want %q", ev.Path, path)
			}
			if ev.Event == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}

func TestWatcherDebouncesRapidWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "burst.txt")
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	w, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, errs := w.Watch(ctx)
	go drainErrors(t, errs)

	// Rapid-fire writes within the debounce window should collapse into one F:WRITE.
	for i := 0; i < 5; i++ {
		if err := os.WriteFile(path, []byte("burst"), 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	writeCount := 0
	timeout := time.After(1 * time.Second)
collect:
	for {
		select {
		case ev := <-events:
			if ev.Event == event.Write {
				writeCount++
			}
		case <-timeout:
			break collect
		}
	}
	if writeCount != 1 {
		t.Fatalf("write events = %d, want 1 (debounced)", writeCount)
	}
}

func drainErrors(t *testing.T, errs <-chan error) {
	t.Helper()
	for err := range errs {
		t.Logf("watcher error: %v", err)
	}
}
