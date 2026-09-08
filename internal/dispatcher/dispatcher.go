package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ss3/internal/event"
	"github.com/ss3/internal/subscription"
)

// deliveryTimeout bounds a single webhook POST.
const deliveryTimeout = 5 * time.Second

// Dispatcher fans an event out to every subscription that wants it.
type Dispatcher struct {
	store  subscription.Store
	client *http.Client
}

func New(store subscription.Store) *Dispatcher {
	return &Dispatcher{
		store:  store,
		client: &http.Client{},
	}
}

// Dispatch delivers ev to every matching subscription. Deliveries are
// fire-and-forget: a failed POST is logged and dropped, with no retry.
func (d *Dispatcher) Dispatch(ctx context.Context, ev event.Event) {
	body, err := json.Marshal(ev)
	if err != nil {
		log.Printf("dispatcher: marshal event %s: %v", ev.ID, err)
		return
	}

	for _, sub := range d.store.All() {
		if !sub.Wants(ev.Event) {
			continue
		}
		go d.deliver(ctx, sub, body)
	}
}

func (d *Dispatcher) deliver(ctx context.Context, sub subscription.Subscription, body []byte) {
	ctx, cancel := context.WithTimeout(ctx, deliveryTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.URL, bytes.NewReader(body))
	if err != nil {
		log.Printf("dispatcher: build request for %s: %v", sub.URL, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		log.Printf("dispatcher: deliver to %s: %v", sub.URL, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("dispatcher: %s responded %s", sub.URL, statusText(resp))
	}
}

func statusText(resp *http.Response) string {
	return fmt.Sprintf("%d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
}
