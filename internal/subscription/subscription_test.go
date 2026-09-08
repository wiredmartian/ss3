package subscription

import (
	"sync"
	"testing"

	"github.com/ss3/internal/config"
	"github.com/ss3/internal/event"
)

func TestSubscriptionWants(t *testing.T) {
	sub := Subscription{
		ID:  "1",
		URL: "https://client.example.com/hook",
		Events: map[event.Type]bool{
			event.Create: true,
			event.Write:  true,
		},
	}

	cases := []struct {
		t    event.Type
		want bool
	}{
		{event.Create, true},
		{event.Write, true},
		{event.Remove, false},
		{event.Rename, false},
		{event.Chmod, false},
	}
	for _, c := range cases {
		if got := sub.Wants(c.t); got != c.want {
			t.Errorf("Wants(%q) = %v, want %v", c.t, got, c.want)
		}
	}
}

func TestSubscriptionWantsNilEvents(t *testing.T) {
	var sub Subscription // zero value: Events is a nil map
	if sub.Wants(event.Create) {
		t.Fatal("Wants on zero-value Subscription = true, want false")
	}
}

func TestNewMemoryBuildsSubscriptionsFromConfig(t *testing.T) {
	cfg := &config.Config{
		WatchDir: "./data",
		Subscriptions: []config.SubscriptionConfig{
			{
				URL:    "https://a.example.com/hook",
				Events: []string{"CREATE", "WRITE"},
			},
			{
				URL:    "https://b.example.com/hook",
				Events: []string{"REMOVE"},
			},
		},
	}

	m, err := NewMemory(cfg)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}

	subs := m.All()
	if len(subs) != 2 {
		t.Fatalf("len(All()) = %d, want 2", len(subs))
	}

	a, b := subs[0], subs[1]
	if a.URL != "https://a.example.com/hook" {
		t.Errorf("subs[0].URL = %q, want https://a.example.com/hook", a.URL)
	}
	if !a.Wants(event.Create) || !a.Wants(event.Write) {
		t.Errorf("subs[0].Events = %v, want CREATE and WRITE set", a.Events)
	}
	if a.Wants(event.Remove) {
		t.Errorf("subs[0] unexpectedly wants REMOVE")
	}

	if b.URL != "https://b.example.com/hook" {
		t.Errorf("subs[1].URL = %q, want https://b.example.com/hook", b.URL)
	}
	if !b.Wants(event.Remove) {
		t.Errorf("subs[1].Events = %v, want REMOVE set", b.Events)
	}
	if b.Wants(event.Create) {
		t.Errorf("subs[1] unexpectedly wants CREATE")
	}
}

func TestNewMemoryAssignsUniqueIDs(t *testing.T) {
	cfg := &config.Config{
		Subscriptions: []config.SubscriptionConfig{
			{URL: "https://a.example.com/hook", Events: []string{"CREATE"}},
			{URL: "https://b.example.com/hook", Events: []string{"CREATE"}},
		},
	}

	m, err := NewMemory(cfg)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}

	subs := m.All()
	if len(subs) != 2 {
		t.Fatalf("len(All()) = %d, want 2", len(subs))
	}
	if subs[0].ID == "" || subs[1].ID == "" {
		t.Fatalf("expected non-empty IDs, got %q and %q", subs[0].ID, subs[1].ID)
	}
	if subs[0].ID == subs[1].ID {
		t.Fatalf("expected unique IDs, got same ID %q for both subscriptions", subs[0].ID)
	}
}

func TestNewMemoryNoSubscriptions(t *testing.T) {
	cfg := &config.Config{WatchDir: "./data"}

	m, err := NewMemory(cfg)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}
	if got := m.All(); len(got) != 0 {
		t.Fatalf("All() = %v, want empty", got)
	}
}

// TestNewMemorySkipsUnknownEventDefensively exercises the defensive branch in
// NewMemory that skips an event name unknown to config.EventType. In normal
// operation config.Load already rejects unknown event names before NewMemory
// ever sees them, but NewMemory is constructed directly here (bypassing
// config.Load) to make sure the fallback itself is correct: it must skip the
// bad name rather than, say, inserting a zero-value event.Type key into the
// subscription's Events map.
func TestNewMemorySkipsUnknownEventDefensively(t *testing.T) {
	cfg := &config.Config{
		Subscriptions: []config.SubscriptionConfig{
			{URL: "https://a.example.com/hook", Events: []string{"CREATE", "BOGUS"}},
		},
	}

	m, err := NewMemory(cfg)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}

	subs := m.All()
	if len(subs) != 1 {
		t.Fatalf("len(All()) = %d, want 1", len(subs))
	}
	sub := subs[0]
	if len(sub.Events) != 1 || !sub.Wants(event.Create) {
		t.Fatalf("Events = %v, want only CREATE set", sub.Events)
	}
	// The zero value of event.Type is "": confirm the unknown name did not
	// leak in as a bogus map key.
	if sub.Events[""] {
		t.Fatalf("Events contains a zero-value event.Type key: %v", sub.Events)
	}
}

func TestMemoryAllReturnsDefensiveCopy(t *testing.T) {
	cfg := &config.Config{
		Subscriptions: []config.SubscriptionConfig{
			{URL: "https://a.example.com/hook", Events: []string{"CREATE"}},
		},
	}
	m, err := NewMemory(cfg)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}

	got := m.All()
	got[0].URL = "mutated"

	again := m.All()
	if again[0].URL == "mutated" {
		t.Fatal("mutating the slice returned by All() affected internal state; All() must return a copy")
	}
}

// TestMemoryAllConcurrentAccess exercises All() from many goroutines at once
// under -race to confirm the RWMutex correctly guards concurrent reads.
func TestMemoryAllConcurrentAccess(t *testing.T) {
	cfg := &config.Config{
		Subscriptions: []config.SubscriptionConfig{
			{URL: "https://a.example.com/hook", Events: []string{"CREATE", "WRITE"}},
			{URL: "https://b.example.com/hook", Events: []string{"REMOVE"}},
		},
	}
	m, err := NewMemory(cfg)
	if err != nil {
		t.Fatalf("NewMemory: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			subs := m.All()
			if len(subs) != 2 {
				t.Errorf("All() len = %d, want 2", len(subs))
			}
		}()
	}
	wg.Wait()
}

func TestStoreInterfaceSatisfiedByMemory(t *testing.T) {
	var _ Store = (*Memory)(nil)
}
