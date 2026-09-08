package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/ss3/internal/config"
	"github.com/ss3/internal/dispatcher"
	"github.com/ss3/internal/subscription"
	"github.com/ss3/internal/watcher"
)

func main() {
	configPath := flag.String("config", "dev.yaml", "path to dev.yaml")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	store, err := subscription.NewMemory(cfg)
	if err != nil {
		log.Fatalf("build subscription store: %v", err)
	}

	w, err := watcher.New(cfg.WatchDir)
	if err != nil {
		log.Fatalf("start watcher: %v", err)
	}

	d := dispatcher.New(store)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("ss3: watching %q with %d subscription(s)", cfg.WatchDir, len(store.All()))

	events, errs := w.Watch(ctx)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return
			}
			log.Printf("ss3: %s %s", ev.Event, ev.Path)
			d.Dispatch(ctx, ev)

		case err, ok := <-errs:
			if !ok {
				return
			}
			log.Printf("ss3: watcher error: %v", err)

		case <-ctx.Done():
			log.Print("ss3: shutting down")
			return
		}
	}
}
