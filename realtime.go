// Package realtime is togo's default SSE realtime provider. Blank-import (or
// `togo install togo-framework/realtime`) to register it with the kernel.
package realtime

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/togo-framework/togo"
	trealtime "github.com/togo-framework/togo/realtime"
)

func init() {
	togo.RegisterProviderFunc("realtime", togo.PriorityService, func(k *togo.Kernel) error {
		k.Realtime = NewBroker()
		return nil
	})
}

type broker struct {
	mu      sync.RWMutex
	clients map[chan string]struct{}
}

// NewBroker creates an SSE broker.
func NewBroker() trealtime.Broker { return &broker{clients: map[chan string]struct{}{}} }

func (b *broker) Publish(event, data string) {
	msg := fmt.Sprintf("event: %s\ndata: %s\n\n", event, data)
	b.mu.RLock()
	for c := range b.clients {
		select {
		case c <- msg:
		default:
		}
	}
	b.mu.RUnlock()
}

func (b *broker) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		ch := make(chan string, 16)
		b.mu.Lock()
		b.clients[ch] = struct{}{}
		b.mu.Unlock()
		defer func() {
			b.mu.Lock()
			delete(b.clients, ch)
			b.mu.Unlock()
		}()
		for {
			select {
			case <-r.Context().Done():
				return
			case msg := <-ch:
				fmt.Fprint(w, msg)
				flusher.Flush()
			}
		}
	}
}
