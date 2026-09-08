package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ss3/internal/event"
)

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, `
watch_dir: ./data
subscriptions:
  - url: https://client.example.com/hook
    events: [CREATE, WRITE, REMOVE, RENAME, CHMOD]
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.WatchDir != "./data" {
		t.Fatalf("WatchDir = %q, want ./data", cfg.WatchDir)
	}
	if len(cfg.Subscriptions) != 1 {
		t.Fatalf("len(Subscriptions) = %d, want 1", len(cfg.Subscriptions))
	}
	sub := cfg.Subscriptions[0]
	if sub.URL != "https://client.example.com/hook" {
		t.Fatalf("URL = %q", sub.URL)
	}
	want := []string{"CREATE", "WRITE", "REMOVE", "RENAME", "CHMOD"}
	if len(sub.Events) != len(want) {
		t.Fatalf("Events = %v, want %v", sub.Events, want)
	}
}

func TestLoadMissingWatchDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, `subscriptions: []`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load: want error for missing watch_dir")
	}
}

func TestLoadUnknownEvent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	writeFile(t, path, `
watch_dir: ./data
subscriptions:
  - url: https://client.example.com/hook
    events: [BOGUS]
`)

	if _, err := Load(path); err == nil {
		t.Fatal("Load: want error for unknown event name")
	}
}

func TestEventType(t *testing.T) {
	cases := map[string]event.Type{
		"CREATE": event.Create,
		"WRITE":  event.Write,
		"REMOVE": event.Remove,
		"RENAME": event.Rename,
		"CHMOD":  event.Chmod,
	}
	for name, want := range cases {
		got, ok := EventType(name)
		if !ok || got != want {
			t.Errorf("EventType(%q) = %q, %v; want %q, true", name, got, ok, want)
		}
	}
	if _, ok := EventType("BOGUS"); ok {
		t.Error("EventType(\"BOGUS\") ok = true, want false")
	}
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
