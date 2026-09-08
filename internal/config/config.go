package config

import (
	"fmt"
	"os"

	"github.com/ss3/internal/event"
	"gopkg.in/yaml.v3"
)

var knownEvents = map[string]event.Type{
	"CREATE": event.Create,
	"WRITE":  event.Write,
	"REMOVE": event.Remove,
	"RENAME": event.Rename,
	"CHMOD":  event.Chmod,
}

type SubscriptionConfig struct {
	URL    string   `yaml:"url"`
	Events []string `yaml:"events"`
}

type Config struct {
	WatchDir      string               `yaml:"watch_dir"`
	Subscriptions []SubscriptionConfig `yaml:"subscriptions"`
}

// Load reads and parses the YAML config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %q: %w", path, err)
	}

	if cfg.WatchDir == "" {
		return nil, fmt.Errorf("config %q: watch_dir is required", path)
	}

	for i, sub := range cfg.Subscriptions {
		if sub.URL == "" {
			return nil, fmt.Errorf("config %q: subscriptions[%d]: url is required", path, i)
		}
		for _, name := range sub.Events {
			if _, ok := knownEvents[name]; !ok {
				return nil, fmt.Errorf("config %q: subscriptions[%d]: unknown event %q", path, i, name)
			}
		}
	}

	return &cfg, nil
}

// EventType converts a config file event name (e.g. "CREATE") to its
// event.Type (e.g. "F:CREATE").
func EventType(name string) (event.Type, bool) {
	t, ok := knownEvents[name]
	return t, ok
}
