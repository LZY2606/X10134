// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/nbio/logging"
	"github.com/lesismal/nbio/mempool"
)

func init() {
	logging.SetLevel(logging.LevelNone)
}

// ---------------------------------------------------------------------------
// Generic helpers
// ---------------------------------------------------------------------------

const lifecycleWatchdog = 5 * time.Second

// must0 fails t if err is non-nil.
func must0(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// signal is a one-shot non-blocking notification.
type signal struct {
	once sync.Once
	ch   chan struct{}
}

func newSignal() *signal { return &signal{ch: make(chan struct{})} }

func (s *signal) fire()      { s.once.Do(func() { close(s.ch) }) }
func (s *signal) waitC() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.ch
}

func waitSignal(t *testing.T, what string, sigs ...*signal) {
	t.Helper()
	timer := time.NewTimer(lifecycleWatchdog)
	defer timer.Stop()
	for _, s := range sigs {
		if s == nil {
			continue
		}
		select {
		case <-s.ch:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// waitCh waits for a generic channel with the test watchdog.
func waitCh(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleWatchdog):
		t.Fatalf("timed out waiting for %s", what)
	}
}

// stopEngine runs g.Stop in a goroutine and fails if it hangs.
func stopEngine(t *testing.T, g *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	waitCh(t, done, "Engine.Stop to return")
}

// waitNoNBioGoroutines asserts that nbio-managed goroutines (pollers,
// listener, conn jobs) have all exited. The success path completes as soon as
// the stacks are clean (usually a few hundred microseconds); the timeout is
// only a failure watchdog.
func waitNoNBioGoroutines(t *testing.T) {
	t.Helper()
	runtime.GC()
	deadline := time.Now().Add(lifecycleWatchdog)
	var bad []byte
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		stacks := strings.Split(string(buf[:n]), "\n\n")
		bad = bad[:0]
		for _, st := range stacks {
			if strings.Contains(st, "github.com/lesismal/nbio.") &&
				(strings.Contains(st, "(*poller).") ||
					strings.Contains(st, "(*Engine).") ||
					strings.Contains(st, "(*Conn).execute") ||
					strings.Contains(st, "(*timer.Timer).Async")) {
				bad = append(bad, st...)
				bad = append(bad, '\n')
			}
		}
		if len(bad) == 0 {
			return
		}
		time.Sleep(time.Millisecond * 5)
	}
	t.Fatalf("nbio goroutines leaked:\n%s", string(bad))
}

// closeErrIsTerminal reports whether err belongs to the terminal peer-close
// error family (clean EOF, connection reset/broken pipe) observed after the
// peer shuts the socket down. Exact errno varies by platform and whether the
// peer still had unread data, so the class rather than one specific value is
// part of the contract.
func closeErrIsTerminal(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "eof") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "shutdown")
}

// ---------------------------------------------------------------------------
// Tracking allocator: counts Malloc/Free and detects use-after-free /
// double-free of write buffers.
// ---------------------------------------------------------------------------

const (
	trackFreeBucketSize = 4096
	trackPoison         = 0xDD
)

type trackingAllocator struct {
	mu         sync.Mutex
	open       map[*[]byte]string // live buffers -> allocation note
	mallocN    int64
	freeN      int64
	maxOpen    int64
	outstanding int64
	quarantine bool
}

func newTrackingAllocator(quarantine bool) *trackingAllocator {
	return &trackingAllocator{
		open:       map[*[]byte]string{},
		quarantine: quarantine,
	}
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	var buf []byte
	if size <= trackFreeBucketSize {
		buf = make([]byte, trackFreeBucketSize)[:size]
	} else {
		buf = make([]byte, size)
	}
	pbuf := &buf
	a.mu.Lock()
	a.open[pbuf] = ""
	a.mallocN++
	n := int64(len(a.open))
	if n > a.maxOpen {
		a.maxOpen = n
	}
	a.outstanding = n
	a.mu.Unlock()
	return pbuf
}

func (a *trackingAllocator) poison(pbuf *[]byte) {
	b := *pbuf
	for i := range b[:cap(b)] {
		b[:cap(b)][i] = trackPoison
	}
}

func (a *trackingAllocator) Free(pbuf *[]byte) {
	a.mu.Lock()
	note, ok := a.open[pbuf]
	if !ok {
		a.mu.Unlock()
		panic(fmt.Sprintf("trackingAllocator: free of unallocated/double-freed buffer %p", pbuf))
	}
	delete(a.open, p)
	a.freeN++
	a.outstanding = int64(len(a.open))
	q := a.quarantine
	a.mu.Unlock()
	if q && cap(*pbuf) <= trackFreeBucketSize {
		a.poison(pbuf)
	}
	_ = note
}

func (a *trackingAllocator) Realloc(pbuf *[]byte, size int) *[]byte {
	if cap(*pbuf) >= size {
		*pbuf = (*pbuf)[:size]
		return pbuf
	}
	nb := a.Malloc(size)
	copy(*nb, *pbuf)
	a.Free(pbuf)
	return nb
}

func (a *trackingAllocator) Append(pbuf *[]byte, more ...byte) *[]byte {
	if cap(*pbuf)-len(*pbuf) >= len(more) {
		*pbuf = append(*pbuf, more...)
		return pbuf
	}
	nb := a.Malloc(len(*pbuf) + len(more))
	copy(*nb, *pbuf)
	copy((*nb)[len(*pbuf):], more)
	a.Free(pbuf)
	return nb
}

func (a *trackingAllocator) AppendString(pbuf *[]byte, more string) *[]byte {
	return a.Append(pbuf, []byte(more)...)
}

func (a *trackingAllocator) snapshot() (malloc, free, openNow, maxOpen int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mallocN, a.freeN, int64(len(a.open)), a.maxOpen
}

func (a *trackingAllocator) assertBalanced(t *testing.T, what string) {
	t.Helper()
	a.mu.Lock()
	opens := len(a.open)
	ma, fr := a.mallocN, a.freeN
	a.mu.Unlock()
	if opens != 0 {
		t.Fatalf("%s: %d write buffers still outstanding (malloc=%d free=%d)", what, opens, ma, fr)
	}
}

var _ mempool.Allocator = (*trackingAllocator)(nil)

// ---------------------------------------------------------------------------
// Per-conn event tracing
// ---------------------------------------------------------------------------

type connTrace struct {
	mu      sync.Mutex
	events  []string
	opens   int
	datas   int
	closes  int
	closeErrs []error
}

func (tr *connTrace) record(ev string) {
	tr.mu.Lock()
	tr.events = append(tr.events, ev)
	tr.mu.Unlock()
}

func (tr *connTrace) onOpen() {
	tr.mu.Lock()
	tr.events = append(tr.events, "open")
	tr.opens++
	tr.mu.Unlock()
}

func (tr *connTrace) onData() {
	tr.mu.Lock()
	tr.events = append(tr.events, "data")
	tr.datas++
	tr.mu.Unlock()
}

func (tr *connTrace) onClose(err error) {
	tr.mu.Lock()
	tr.events = append(tr.events, "close")
	tr.closes++
	tr.closeErrs = append(tr.closeErrs, err)
	tr.mu.Unlock()
}

func (tr *connTrace) assertSingleLifecycle(t *testing.T, what string) {
	t.Helper()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.opens != 1 {
		t.Fatalf("%s: OnOpen count = %d, want 1", what, tr.opens)
	}
	if tr.closes != 1 {
		t.Fatalf("%s: OnClose count = %d, want 1", what, tr.closes)
	}
	// same-conn event order: a single open first, zero or more data, one close last.
	var seq []string
	for _, ev := range tr.events {
		switch ev {
		case "open":
			if len(seq) != 0 {
				t.Fatalf("%s: open event not first: %v", what, tr.events)
			}
		case "data":
			if len(seq) == 0 || seq[len(seq)-1] == "close" {
				t.Fatalf("%s: data event outside open..close window: %v", what, tr.events)
			}
		case "close":
			if len(seq) == 0 || seq[len(seq)-1] == "close" {
				t.Fatalf("%s: close event invalid/duplicated: %v", what, tr.events)
			}
		}
		seq = append(seq, ev)
	}
}

// traceRegistry maps *Conn -> *connTrace.
type traceRegistry struct {
	mu     sync.Mutex
	traces map[*Conn]*connTrace
}

func newTraceRegistry() *traceRegistry {
	return &traceRegistry{traces: map[*Conn]*connTrace{}}
}

func (r *traceRegistry) get(c *Conn) *connTrace {
	r.mu.Lock()
	tr := r.traces[c]
	r.mu.Unlock()
	return tr
}

func (r *traceRegistry) getOrCreate(c *Conn) *connTrace {
	r.mu.Lock()
	tr := r.traces[c]
	if tr == nil {
		tr = &connTrace{}
		r.traces[c] = tr
	}
	r.mu.Unlock()
	return tr
}

func (r *traceRegistry) each(f func(*Conn, *connTrace)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for c, tr := range r.traces {
		f(c, tr)
	}
}

func (r *traceRegistry) totals() (open, data, close int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tr := range r.traces {
		tr.mu.Lock()
		open += tr.opens
		data += tr.datas
		close += tr.closes
		tr.mu.Unlock()
	}
	return
}

// ---------------------------------------------------------------------------
// Engine builders
// ---------------------------------------------------------------------------

type lifecycleHooks struct {
	onOpen  func(c *Conn)
	onData  func(c *Conn, data []byte)
	onClose func(c *Conn, err error)
}

func newLifecycleEngine(t *testing.T, npoller int, alloc mempool.Allocator, reg *traceRegistry, hooks lifecycleHooks) *Engine {
	t.Helper()
	g := NewEngine(Config{
		Name:          "lifecycle-" + t.Name(),
		Network:       "tcp",
		Addrs:         []string{"127.0.0.1:0"},
		NPoller:       npoller,
		BodyAllocator: alloc,
	})
	g.OnOpen(func(c *Conn) {
		tr := reg.getOrCreate(c)
		tr.onOpen()
		if hooks.onOpen != nil {
			hooks.onOpen(c)
		}
	})
	g.OnData(func(c *Conn, data []byte) {
		tr := reg.getOrCreate(c)
		tr.onData()
		if hooks.onData != nil {
			hooks.onData(c, data)
		}
	})
	g.OnClose(func(c *Conn, err error) {
		tr := reg.getOrCreate(c)
		tr.onClose(err)
		if hooks.onClose != nil {
			hooks.onClose(c, err)
		}
	})
	must0(t, g.Start())
	return g
}

// dialRaw dials the engine listener with a plain net.Conn and shrinks both
// socket buffers to make EAGAIN / queued writes easy to reach deterministically.
func dialRaw(t *testing.T, addr string, bufSize int) net.Conn {
	t.Helper()
	nc, err := net.Dial("tcp", addr)
	must0(t, err)
	tc := nc.(*net.TCPConn)
	if bufSize > 0 {
		_ = tc.SetReadBuffer(bufSize)
		_ = tc.SetWriteBuffer(bufSize)
	}
	_ = tc.SetNoDelay(true)
	return nc
}

// drain reads nc until a terminal read error; it does not interpret payloads.
func drain(t *testing.T, nc net.Conn, buf []byte) {
	t.Helper()
	for {
		_ = nc.SetReadDeadline(time.Now().Add(lifecycleWatchdog))
		_, err := nc.Read(buf)
		if err != nil {
			return
		}
	}
}

// waitForBlockedGoroutine blocks until cond observes a goroutine whose stack
// contains substr. Used to verify a callback is parked at an exact phase.
func waitForBlockedGoroutine(t *testing.T, substr string) {
	t.Helper()
	deadline := time.Now().Add(lifecycleWatchdog)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), substr) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("no goroutine blocked at %q", substr)
}

var _ = os.Getpid
var _ atomic.Int32
