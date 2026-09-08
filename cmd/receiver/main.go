package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net/http"

	"github.com/ss3/internal/event"
)

func main() {
	addr := flag.String("addr", ":8002", "address to listen on")
	flag.Parse()

	http.HandleFunc("/", handle)

	log.Printf("receiver: listening on %s", *addr)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatalf("receiver: %v", err)
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}

	var ev event.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		log.Printf("receiver: invalid payload: %v\n%s", err, body)
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	pretty, _ := json.MarshalIndent(ev, "", "  ")
	log.Printf("receiver: received notification\n%s", pretty)

	w.WriteHeader(http.StatusOK)
}
