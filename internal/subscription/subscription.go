package subscription

import (
	"sync"

	"github.com/google/uuid"
	"github.com/ss3/internal/config"
	"github.com/ss3/internal/event"
)

type Subscription struct {
	ID     string
	URL    string
	Events map[event.Type]bool
}

// Wants reports whether this subscription is registered for t.
func (s Subscription) Wants(t event.Type) bool {
	return s.Events[t]
}

// Store looks up the current set of subscriptions. Phase 1 ships an
// in-memory implementation; a later phase can back this with SQLite without
// changing any caller.
type Store interface {
	All() []Subscription
}

type Memory struct {
	mu   sync.RWMutex
	subs []Subscription
}

func NewMemory(cfg *config.Config) (*Memory, error) {
	m := &Memory{}
	for _, sc := range cfg.Subscriptions {
		events := make(map[event.Type]bool, len(sc.Events))
		for _, name := range sc.Events {
			t, ok := config.EventType(name)
			if !ok {
				continue // config.Load already validates this; defensive only.
			}
			events[t] = true
		}
		m.subs = append(m.subs, Subscription{
			ID:     uuid.NewString(),
			URL:    sc.URL,
			Events: events,
		})
	}
	return m, nil
}

func (m *Memory) All() []Subscription {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Subscription, len(m.subs))
	copy(out, m.subs)
	return out
}
