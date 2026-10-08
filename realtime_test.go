package realtime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

const wait = 2 * time.Second

func newTestBroker(t *testing.T, keepAlive time.Duration) (*broker, *httptest.Server) {
	t.Helper()
	b := NewBroker(WithKeepAlive(keepAlive)).(*broker)
	srv := httptest.NewServer(b.Handler())
	t.Cleanup(srv.Close)
	return b, srv
}

// readLine reads one line or fails the test after the deadline.
func readLine(t *testing.T, r *bufio.Reader) string {
	t.Helper()
	type res struct {
		s   string
		err error
	}
	c := make(chan res, 1)
	go func() { s, err := r.ReadString('\n'); c <- res{s, err} }()
	select {
	case v := <-c:
		if v.err != nil {
			t.Fatalf("read: %v", v.err)
		}
		return strings.TrimRight(v.s, "\r\n")
	case <-time.After(wait):
		t.Fatal("timed out waiting for a line")
		return ""
	}
}

func waitClients(t *testing.T, b *broker, n int) {
	t.Helper()
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		b.mu.RLock()
		got := len(b.clients)
		b.mu.RUnlock()
		if got == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected %d subscribers", n)
}

func TestHeadersAndPreambleFlushedBeforeAnyEvent(t *testing.T) {
	_, srv := newTestBroker(t, time.Hour)
	// http.Get returns only once the response headers arrive: if the handler
	// did not flush on connect this would block (no event is ever published).
	done := make(chan *http.Response, 1)
	go func() {
		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Error(err)
			close(done)
			return
		}
		done <- resp
	}()
	var resp *http.Response
	select {
	case resp = <-done:
		if resp == nil {
			return
		}
	case <-time.After(wait):
		t.Fatal("headers were not flushed on connect")
	}
	defer resp.Body.Close()
	h := resp.Header
	for k, want := range map[string]string{
		"Content-Type":      "text/event-stream",
		"Cache-Control":     "no-cache, no-transform",
		"X-Accel-Buffering": "no",
	} {
		if got := h.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if line := readLine(t, bufio.NewReader(resp.Body)); !strings.HasPrefix(line, "retry:") {
		t.Errorf("first line = %q, want retry preamble", line)
	}
}

func TestEventsStreamedInOrder(t *testing.T) {
	b, srv := newTestBroker(t, time.Hour)
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	readLine(t, r) // retry
	readLine(t, r) // blank
	waitClients(t, b, 1)
	b.Publish("a", "1")
	b.Publish("b", "2")
	for _, want := range []string{"event: a", "data: 1", "", "event: b", "data: 2", ""} {
		if got := readLine(t, r); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestKeepAliveEmitted(t *testing.T) {
	_, srv := newTestBroker(t, 20*time.Millisecond)
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	for i := 0; i < 5; i++ {
		if readLine(t, r) == ": ping" {
			return
		}
	}
	t.Fatal("no keep-alive comment received")
}

func TestKeepAliveDisabled(t *testing.T) {
	b := NewBroker(WithKeepAlive(0)).(*broker)
	if b.keepAlive != 0 {
		t.Fatalf("keepAlive = %v, want 0", b.keepAlive)
	}
	if NewBroker().(*broker).keepAlive != DefaultKeepAlive {
		t.Fatal("default keep-alive not applied")
	}
}

func TestDisconnectRemovesSubscriber(t *testing.T) {
	b, srv := newTestBroker(t, 10*time.Millisecond)
	for i := 0; i < 3; i++ {
		resp, err := http.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		waitClients(t, b, 1)
		resp.Body.Close()
		waitClients(t, b, 0)
	}
}

type noFlushWriter struct {
	h    http.Header
	code int
	body strings.Builder
}

func (w *noFlushWriter) Header() http.Header         { return w.h }
func (w *noFlushWriter) WriteHeader(c int)           { w.code = c }
func (w *noFlushWriter) Write(p []byte) (int, error) { return w.body.Write(p) }

func TestNonFlushableWriterGetsClearError(t *testing.T) {
	b := NewBroker().(*broker)
	w := &noFlushWriter{h: http.Header{}}
	b.Handler()(w, httptest.NewRequest("GET", "/events", nil))
	if w.code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500", w.code)
	}
	if !strings.Contains(w.body.String(), "streaming unsupported") {
		t.Fatalf("body = %q", w.body.String())
	}
	if strings.Contains(w.body.String(), "retry:") {
		t.Fatal("stream preamble leaked into error response")
	}
	if ct := w.h.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.clients) != 0 {
		t.Fatal("subscriber registered despite error")
	}
}

func TestThroughReverseProxy(t *testing.T) {
	b, backend := newTestBroker(t, 20*time.Millisecond)
	target, _ := url.Parse(backend.URL)
	rp := httputil.NewSingleHostReverseProxy(target)
	rp.FlushInterval = -1 // what a streaming-aware proxy does for event streams
	proxy := httptest.NewServer(rp)
	defer proxy.Close()

	resp, err := http.Get(proxy.URL) // blocks until headers cross the proxy
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type via proxy = %q", ct)
	}
	r := bufio.NewReader(resp.Body)
	if line := readLine(t, r); !strings.HasPrefix(line, "retry:") {
		t.Fatalf("first bytes via proxy = %q", line)
	}
	waitClients(t, b, 1)
	b.Publish("x", "y")
	for i := 0; i < 10; i++ {
		if readLine(t, r) == "event: x" {
			return
		}
	}
	t.Fatal("event not delivered through proxy")
}

// reactiveWriter simulates a client that publishes the moment it sees the
// stream open: its Flush publishes an event as soon as the retry preamble has
// been flushed, then ends the request once that event has been written.
type reactiveWriter struct {
	b         *broker
	cancel    context.CancelFunc
	h         http.Header
	mu        sync.Mutex
	body      strings.Builder
	published bool
	failEvent bool // fail the write of the published event
}

func (w *reactiveWriter) Header() http.Header { return w.h }
func (w *reactiveWriter) WriteHeader(int)     {}
func (w *reactiveWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failEvent && bytes.Contains(p, []byte("event:")) {
		return 0, errors.New("broken pipe")
	}
	return w.body.Write(p)
}
func (w *reactiveWriter) Flush() {
	w.mu.Lock()
	body := w.body.String()
	publish := !w.published && strings.Contains(body, "retry:")
	if publish {
		w.published = true
	}
	w.mu.Unlock()
	if publish {
		w.b.Publish("first", "1")
	}
	if strings.Contains(body, "event: first") {
		w.cancel()
	}
}

func TestEventPublishedRightAfterPreambleIsDelivered(t *testing.T) {
	b := NewBroker(WithKeepAlive(time.Hour)).(*broker)
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	w := &reactiveWriter{b: b, cancel: cancel, h: http.Header{}}
	b.Handler()(w, httptest.NewRequest("GET", "/events", nil).WithContext(ctx))
	if !strings.Contains(w.body.String(), "event: first\ndata: 1\n\n") {
		t.Fatalf("event published right after the preamble was lost; stream = %q", w.body.String())
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.clients) != 0 {
		t.Fatal("subscriber not removed after the request ended")
	}
}

// deadlineErrWriter supports flushing but fails to clear the write deadline
// with an error other than http.ErrNotSupported.
type deadlineErrWriter struct {
	noFlushWriter
}

func (w *deadlineErrWriter) Flush()                           {}
func (w *deadlineErrWriter) SetWriteDeadline(time.Time) error { return errors.New("conn closed") }

func TestWriteDeadlineFailureIsLoggedNotSilent(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	b := NewBroker().(*broker)
	w := &deadlineErrWriter{noFlushWriter{h: http.Header{}}}
	b.Handler()(w, httptest.NewRequest("GET", "/events", nil))
	if w.code != http.StatusInternalServerError {
		t.Fatalf("code = %d, want 500 instead of an empty 200", w.code)
	}
	if strings.Contains(w.body.String(), "retry:") {
		t.Fatal("stream preamble leaked into error response")
	}
	if ct := w.h.Get("Content-Type"); strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("Content-Type = %q", ct)
	}
	if !strings.Contains(logs.String(), "conn closed") {
		t.Fatalf("write-deadline failure not logged; logs = %q", logs.String())
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.clients) != 0 {
		t.Fatal("subscriber registered despite error")
	}
}

func TestWriteFailureRemovesSubscriber(t *testing.T) {
	b := NewBroker(WithKeepAlive(time.Hour)).(*broker)
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	w := &reactiveWriter{b: b, cancel: func() {}, h: http.Header{}, failEvent: true}
	b.Handler()(w, httptest.NewRequest("GET", "/events", nil).WithContext(ctx))
	if ctx.Err() != nil {
		t.Fatal("handler kept streaming after a write failure")
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.clients) != 0 {
		t.Fatal("subscriber not removed after a write failure")
	}
}
