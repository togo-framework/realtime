// Package realtime is togo's default SSE realtime provider. Blank-import (or
// `togo install togo-framework/realtime`) to register it with the kernel.
package realtime

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/togo-framework/togo"
)

func init() {
	togo.RegisterProviderFunc("realtime", togo.PriorityService, func(k *togo.Kernel) error {
		k.Realtime = NewBroker(WithKeepAlive(keepAliveFromEnv()))
		return nil
	})
}

// DefaultKeepAlive is the interval between ": ping" comments on an idle stream.
// It sits below the 30-60s idle timeouts of common proxies (nginx, NPM,
// Cloudflare tunnels, cloud load balancers).
const DefaultKeepAlive = 20 * time.Second

// EnvKeepAlive names the environment variable (a Go duration such as "15s",
// or "0" to disable) that overrides DefaultKeepAlive for the registered provider.
const EnvKeepAlive = "REALTIME_KEEPALIVE"

func keepAliveFromEnv() time.Duration {
	v := os.Getenv(EnvKeepAlive)
	if v == "" {
		return DefaultKeepAlive
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		slog.Warn("realtime: invalid "+EnvKeepAlive+", using default", "value", v, "default", DefaultKeepAlive)
		return DefaultKeepAlive
	}
	return d
}

// Option configures a broker.
type Option func(*broker)

// WithKeepAlive sets the keep-alive comment interval. Zero disables keep-alives.
func WithKeepAlive(d time.Duration) Option { return func(b *broker) { b.keepAlive = d } }

type broker struct {
	mu        sync.RWMutex
	clients   map[chan string]struct{}
	keepAlive time.Duration
}

// NewBroker creates an SSE broker.
func NewBroker(opts ...Option) togo.Broker {
	b := &broker{clients: map[chan string]struct{}{}, keepAlive: DefaultKeepAlive}
	for _, o := range opts {
		o(b)
	}
	return b
}

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

// Handler serves the event stream. It commits the response headers and an
// initial "retry:" line immediately so clients and buffering proxies see the
// stream open, then emits ": ping" comments every keep-alive interval.
//
// The stream is long-lived, so the handler clears the connection write
// deadline via http.ResponseController; an http.Server WriteTimeout would
// otherwise cut the stream when it elapses. Writers that do not support
// deadlines (ErrNotSupported) are left as they are. A ResponseWriter that
// cannot flush gets a 500 instead of a silently buffered stream.
func (b *broker) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache, no-transform")
		h.Set("X-Accel-Buffering", "no") // nginx / Nginx Proxy Manager: do not buffer
		if r.ProtoMajor == 1 {
			h.Set("Connection", "keep-alive") // forbidden on HTTP/2+
		}
		if err := rc.Flush(); err != nil {
			h.Del("Content-Type")
			h.Del("Cache-Control")
			h.Del("X-Accel-Buffering")
			h.Del("Connection")
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		if err := rc.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return
		}
		if _, err := fmt.Fprint(w, "retry: 3000\n\n"); err != nil {
			return
		}
		if err := rc.Flush(); err != nil {
			return
		}

		ch := make(chan string, 16)
		b.mu.Lock()
		b.clients[ch] = struct{}{}
		b.mu.Unlock()
		defer func() {
			b.mu.Lock()
			delete(b.clients, ch)
			b.mu.Unlock()
		}()

		var tick <-chan time.Time
		if b.keepAlive > 0 {
			t := time.NewTicker(b.keepAlive)
			defer t.Stop()
			tick = t.C
		}
		for {
			var out string
			select {
			case <-r.Context().Done():
				return
			case out = <-ch:
			case <-tick:
				out = ": ping\n\n"
			}
			if _, err := fmt.Fprint(w, out); err != nil {
				return
			}
			if err := rc.Flush(); err != nil {
				return
			}
		}
	}
}
