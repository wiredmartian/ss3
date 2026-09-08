package dispatcher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ss3/internal/event"
	"github.com/ss3/internal/subscription"
)

type fakeStore struct {
	subs []subscription.Subscription
}

func (f fakeStore) All() []subscription.Subscription { return f.subs }

func TestDispatchDeliversToMatchingSubscription(t *testing.T) {
	var mu sync.Mutex
	var received event.Event
	got := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode body: %v", err)
		}
		close(got)
	}))
	defer srv.Close()

	store := fakeStore{subs: []subscription.Subscription{
		{ID: "1", URL: srv.URL, Events: map[event.Type]bool{event.Create: true}},
	}}
	d := New(store)

	ev := event.New(event.Create, "/watched/report.csv", 4096)
	d.Dispatch(context.Background(), ev)

	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
	}

	mu.Lock()
	defer mu.Unlock()
	if received.ID != ev.ID || received.Event != ev.Event || received.Path != ev.Path {
		t.Fatalf("received = %+v, want %+v", received, ev)
	}
}

func TestDispatchSkipsNonMatchingSubscription(t *testing.T) {
	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	store := fakeStore{subs: []subscription.Subscription{
		{ID: "1", URL: srv.URL, Events: map[event.Type]bool{event.Remove: true}},
	}}
	d := New(store)

	d.Dispatch(context.Background(), event.New(event.Create, "/watched/report.csv", 0))

	// Give any (incorrect) delivery a moment to happen before asserting it didn't.
	time.Sleep(200 * time.Millisecond)
	if called {
		t.Fatal("dispatcher delivered to a subscription that did not want this event type")
	}
}
