package event

import (
	"time"

	"github.com/google/uuid"
)

// Type identifies the kind of filesystem change, named to match fsnotify's
// own Op constants (prefixed with "F:").
type Type string

const (
	Create Type = "F:CREATE"
	Write  Type = "F:WRITE"
	Remove Type = "F:REMOVE"
	Rename Type = "F:RENAME"
	Chmod  Type = "F:CHMOD"
)

type Event struct {
	ID        string    `json:"id"`
	Event     Type      `json:"event"`
	Path      string    `json:"path"`
	Size      int64     `json:"size,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

func New(t Type, path string, size int64) Event {
	return Event{
		ID:        uuid.NewString(),
		Event:     t,
		Path:      path,
		Size:      size,
		Timestamp: time.Now().UTC(),
	}
}
