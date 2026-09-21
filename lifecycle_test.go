// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/nbio/mempool"
)

// lifecycleTestTimeout is the single upper bound every deterministic wait uses.
// It is only a failure guard; progress is signaled through channels.
const lifecycleTestTimeout = 10 * time.Second

// waitSignal waits for ch or fails the test with a context message.
func waitSignal(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timeout waiting for %s", msg)
	}
}

// trackingAllocator wraps a real Allocator and records every write-cache
// buffer allocation/release so tests can assert:
//   - every malloc is released exactly once (no buffer lost, no double free),
//   - buffers handed to onWrittenSize are never reused/freed before the
//     callback returns (poison is written on free).
type trackingAllocator struct {
	base mempool.Allocator

	mu       sync.Mutex
	live     map[*[]byte]int
	allocs   int
	frees    int
	double   int
	poisoned [][]byte
}

func newTrackingAllocator() *trackingAllocator {
	return &trackingAllocator{
		base: mempool.NewSTD(),
		live: map[*[]byte]int{},
	}
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	p := a.base.Malloc(size)
	a.mu.Lock()
	a.live[p]++
	a.allocs++
	a.mu.Unlock()
	return p
}

func (a *trackingAllocator) Realloc(p *[]byte, size int) *[]byte {
	// nbio never calls Realloc on BodyAllocator; keep the contract anyway.
	return a.base.Realloc(p, size)
}

func (a *trackingAllocator) Append(p *[]byte, more ...byte) *[]byte {
	return a.base.Append(p, more...)
}

func (a *trackingAllocator) AppendString(p *[]byte, more string) *[]byte {
	return a.base.AppendString(p, more)
}

func (a *trackingAllocator) Free(p *[]byte) {
	a.mu.Lock()
	n := a.live[p]
	if n == 0 {
		a.double++
	} else {
		delete(a.live, p)
	}
	a.frees++
	// Poison the backing array: any write callback that still touches this
	// buffer after release observes deterministic garbage instead of stale
	// payload.
	if p != nil {
		b := *p
		for i := range b {
			b[i] = 0xdd
		}
	}
	a.mu.Unlock()
	a.base.Free(p)
}

func (a *trackingAllocator) stats() (live, allocs, frees, double int) {
	a.mu.Lock()
	live, allocs, frees, double = len(a.live), a.allocs, a.frees, a.double
	a.mu.Unlock()
	return
}

// assertBalanced fails unless every allocated write buffer was freed exactly
// once.
func (a *trackingAllocator) assertBalanced(t *testing.T) {
	t.Helper()
	live, allocs, frees, double := a.stats()
	if live != 0 || allocs != frees || double != 0 {
		t.Fatalf("write buffer accounting mismatch: live=%v allocs=%v frees=%v doubleFree=%v",
			live, allocs, frees, double)
	}
}

// assertNoPoison verifies a buffer observed by onWrittenSize is intact, i.e.
// release happened strictly after the write-size callback.
func assertNoPoison(t *testing.T, b []byte, where string) {
	t.Helper()
	for i, v := range b {
		if v == 0xdd {
			t.Fatalf("%s: write callback observed a released/reused buffer at offset %d", where, i)
		}
	}
}

// eventLog records ordered lifecycle callbacks of a single connection and
// guarantees each recorded callback happens exactly once per event kind.
type eventLog struct {
	mu     sync.Mutex
	events []string
	counts map[string]int
}

func newEventLog() *eventLog {
	return &eventLog{counts: map[string]int{}}
}

func (l *eventLog) record(name string) {
	l.mu.Lock()
	l.events = append(l.events, name)
	l.counts[name]++
	l.mu.Unlock()
}

func (l *eventLog) snapshot() ([]string, map[string]int) {
	l.mu.Lock()
	ev := append([]string(nil), l.events...)
	cp := make(map[string]int, len(l.counts))
	for k, v := range l.counts {
		cp[k] = v
	}
	l.mu.Unlock()
	return ev, cp
}

func (l *eventLog) assertCount(t *testing.T, name string, want int) {
	t.Helper()
	_, counts := l.snapshot()
	if got := counts[name]; got != want {
		t.Fatalf("callback %q invoked %d times, want %d; events=%v", name, got, want, l.String())
	}
}

func (l *eventLog) assertOrdered(t *testing.T, names ...string) {
	t.Helper()
	events, _ := l.snapshot()
	idx := 0
	for _, ev := range events {
		if idx < len(names) && ev == names[idx] {
			idx++
		}
	}
	if idx != len(names) {
		t.Fatalf("event order %v does not contain prefix %v", events, names)
	}
}

func (l *eventLog) String() string {
	ev, counts := l.snapshot()
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ev = append(ev, fmt.Sprintf("%sx%d", k, counts[k]))
	}
	return strings.Join(ev, ",")
}

// lifecycleCounters aggregates per-engine callback counts across connections.
type lifecycleCounters struct {
	open  int64
	close int64
}

func (lc *lifecycleCounters) onOpen() int64  { return atomic.AddInt64(&lc.open, 1) }
func (lc *lifecycleCounters) onClose() int64 { return atomic.AddInt64(&lc.close, 1) }

func (lc *lifecycleCounters) balanced() bool {
	return atomic.LoadInt64(&lc.open) == atomic.LoadInt64(&lc.close)
}

// newLifecycleEngine creates a started engine with a tracking allocator and
// the supplied callback hooks. The returned closer fully stops the engine and
// must be deferred by every test.
func newLifecycleEngine(t *testing.T, conf Config, alloc *trackingAllocator,
	onOpen func(c *Conn), onData func(c *Conn, data []byte), onClose func(c *Conn, err error),
) (*Engine, *trackingAllocator, func()) {
	t.Helper()
	if alloc == nil {
		alloc = newTrackingAllocator()
	}
	conf.BodyAllocator = alloc
	g := NewEngine(conf)
	if onOpen != nil {
		g.OnOpen(onOpen)
	}
	if onData != nil {
		g.OnData(onData)
	}
	if onClose != nil {
		g.OnClose(onClose)
	}
	if err := g.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	return g, alloc, func() { g.Stop() }
}

// dialPeer dials the engine and blocks until the server-side connection has
// been registered, so tests start from a known state.
func dialPeer(t *testing.T, g *Engine, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	deadline := time.Now().Add(lifecycleTestTimeout)
	for time.Now().Before(deadline) {
		g.mux.Lock()
		nStd := len(g.connsStd)
		g.mux.Unlock()
		var nUnix int
		if g.connsUnix != nil {
			for _, c := range g.connsUnix {
				if c != nil {
					nUnix++
				}
			}
		}
		if nStd+nUnix > 0 {
			return conn
		}
		runtime.Gosched()
	}
	t.Fatalf("server-side connection never registered")
	return nil
}

// isPeerCloseError reports whether err represents the peer shutting the
// connection down (EOF/reset/closed pipe). The exact error varies by OS and
// by whether data was still queued.
func isPeerCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "forcibly closed")
}

// goroutineStacks returns the full stacks of live goroutines that mention the
// nbio package (poller loops, listener, timer/async workers).
func nbioGoroutineStacks() string {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	var keep []string
	for _, stack := range strings.Split(string(buf[:n]), "\n\n") {
		// A listener parked in Accept is woken by Stop; that is a normal
		// shutdown frame, not a leak.
		if strings.Contains(stack, "acceptorLoop") && strings.Contains(stack, "Accept") {
			continue
		}
		if strings.Contains(stack, "github.com/lesismal/nbio") &&
			!strings.Contains(stack, "_test.go:") {
			keep = append(keep, stack)
		}
	}
	return strings.Join(keep, "\n---\n")
}

// assertNoNbioGoroutines retries briefly (bounded, no sleeps longer than a
// short tick) to let real shutdown progress, then fails if nbio goroutines
// remain.
func assertNoNbioGoroutines(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var stacks string
	for time.Now().Before(deadline) {
		stacks = nbioGoroutineStacks()
		if stacks == "" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("nbio goroutines still alive after Stop:\n%s", stacks)
}

// fillUntilBlocked writes to conn until the kernel send buffer refuses more
// data (write deadline hit). It returns the number written. Used to force the
// peer's nbio write path into EAGAIN so subsequent writes get queued.
func fillUntilBlocked(t *testing.T, conn net.Conn, payload []byte) int {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
	total := 0
	for {
		n, err := conn.Write(payload)
		total += n
		if err != nil {
			break
		}
		if total > 64*1024*1024 {
			t.Fatalf("fillUntilBlocked: send buffer never filled")
		}
	}
	_ = conn.SetWriteDeadline(time.Time{})
	return total
}

// drainReads keeps reading the nbio side's echo until conn is closed; it also
// drains the kernel receive buffer so writes do not stall on flow control.
func drainReads(conn net.Conn, done chan<- struct{}) {
	buf := make([]byte, 64*1024)
	for {
		n, err := conn.Read(buf)
		if n > 0 || err == nil {
			continue
		}
		break
	}
	close(done)
}

// startLifecycleServer starts an echo engine listening on an ephemeral
// loopback port, installs the given callbacks and returns its dial address.
func startLifecycleServer(t *testing.T, npoller int, alloc *trackingAllocator,
	open func(c *Conn), data func(c *Conn, data []byte), closeFn func(c *Conn, err error),
) (*Engine, *trackingAllocator, string, func()) {
	t.Helper()
	g, a, stop := newLifecycleEngine(t, Config{
		Network: "tcp",
		Addrs:   []string{"127.0.0.1:0"},
		NPoller: npoller,
	}, alloc, open, data, closeFn)
	if len(g.Addrs) != 1 {
		stop()
		t.Fatalf("expected 1 listener address, got %v", g.Addrs)
	}
	return g, a, g.Addrs[0], stop
}

var bigPayload = make([]byte, 256*1024)

func init() {
	for i := range bigPayload {
		bigPayload[i] = byte(i%251 + 1) // never 0x00 nor the 0xdd poison
	}
}

// TestLifecycleCloseDuringOnOpen: a different goroutine requests Close while
// the listener thread is still inside OnOpen. Deterministically pinned by a
// barrier: OnOpen signals entered and waits for release; the closer waits for
// entered then calls Close before OnOpen returns.
func TestLifecycleCloseDuringOnOpen(t *testing.T) {
	var (
		serverConn *Conn
		log        = newEventLog()
		counters   lifecycleCounters
		alloc      = newTrackingAllocator()

		openEntered = make(chan struct{})
		releaseOpen = make(chan struct{})
		closeDone   = make(chan struct{})
		onCloseDone = make(chan struct{})
	)

	open := func(c *Conn) {
		serverConn = c
		counters.onOpen()
		log.record("open")
		close(openEntered)
		<-releaseOpen
	}
	closeFn := func(c *Conn, err error) {
		log.record("close")
		counters.onClose()
		close(onCloseDone)
	}

	_, _, addr, stop := startLifecycleServer(t, 2, alloc, open, nil, closeFn)
	defer stop()

	peer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = peer.Close() }()

	waitSignal(t, openEntered, "OnOpen")

	// Another goroutine requests the close while OnOpen has not returned.
	go func() {
		_ = serverConn.Close()
		close(closeDone)
	}()
	waitSignal(t, closeDone, "user Close")

	// Close must be idempotent even at this point.
	if err := serverConn.Close(); err != nil {
		t.Fatalf("second Close returned %v, want nil", err)
	}

	close(releaseOpen)

	waitSignal(t, onCloseDone, "OnClose")

	closed, closeErr := serverConn.IsClosed()
	if !closed {
		t.Fatalf("conn should be closed")
	}
	// Close requested explicitly with nil error: onClose sees nil.
	if closeErr != nil {
		t.Fatalf("closeErr = %v, want nil", closeErr)
	}

	log.assertCount(t, "open", 1)
	log.assertCount(t, "close", 1)
	alloc.assertBalanced(t)
	if !counters.balanced() {
		t.Fatalf("open/close callback mismatch: open=%d close=%d",
			atomic.LoadInt64(&counters.open), atomic.LoadInt64(&counters.close))
	}
	assertNoNbioGoroutines(t)
}

// TestLifecycleQueuedWritesPeerClose: multiple buffers are already sitting in
// the write queue when the peer closes. All queued buffers must be released
// exactly once, OnClose fires once with a peer-close class error, no panic.
func TestLifecycleQueuedWritesPeerClose(t *testing.T) {
	// Build a client engine that connects to a draining peer. The peer never
	// reads, so the nbio client's send buffer fills and writes are queued.
	var (
		log        = newEventLog()
		counters   lifecycleCounters
		alloc      = newTrackingAllocator()
		onCloseCh  = make(chan error, 1)
		serverConn *Conn
		connReady  = make(chan struct{})
	)

	// Raw peer that accepts but never reads, then closes.
	peerLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	peerAccepted := make(chan net.Conn, 1)
	go func() {
		c, aerr := peerLn.Accept()
		if aerr != nil {
			return
		}
		peerAccepted <- c
	}()

	g, _, stop := newLifecycleEngine(t, Config{NPoller: 1}, alloc,
		func(c *Conn) {
			serverConn = c
			counters.onOpen()
			log.record("open")
			close(connReady)
		},
		nil,
		func(c *Conn, cerr error) {
			log.record("close")
			counters.onClose()
			onCloseCh <- cerr
		},
	)
	defer stop()

	raw, err := net.Dial("tcp", peerLn.Addr().String())
	if err != nil {
		t.Fatalf("dial raw peer: %v", err)
	}

	nbc, err := g.AddConn(raw)
	if err != nil {
		t.Fatalf("AddConn: %v", err)
	}
	_ = nbc
	waitSignal(t, connReady, "client OnOpen")

	if writeQueueSupported {
		// Force the nbio conn's send path into EAGAIN by filling both kernel
		// and nbio cache.
		fillConnSendBuffer(t, serverConn, bigPayload)

		// Queue several additional distinct buffers that coalescing cannot
		// merge into the head (each larger than the 64KiB coalesce limit).
		nQueued := 4
		for i := 0; i < nQueued; i++ {
			buf := make([]byte, 200*1024)
			for j := range buf {
				buf[j] = byte(i + 1)
			}
			if _, werr := serverConn.Write(buf); werr != nil {
				t.Fatalf("queued write %d failed: %v", i, werr)
			}
		}

		queued := writeListLen(serverConn)
		if queued < 2 {
			t.Fatalf("expected multiple queued buffers, got %d", queued)
		}
	} else {
		// Synchronous backend: a sub-send-buffer write completes immediately.
		if _, werr := serverConn.Write([]byte("queued-weak")); werr != nil {
			t.Fatalf("write failed: %v", werr)
		}
	}

	// Peer closes without reading.
	peer := <-peerAccepted
	_ = peer.Close()

	var cerr error
	select {
	case cerr = <-onCloseCh:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("OnClose never fired after peer closed")
	}
	if !isPeerCloseError(cerr) {
		t.Fatalf("OnClose err = %v, want peer-close class", cerr)
	}

	log.assertCount(t, "open", 1)
	log.assertCount(t, "close", 1)

	// Second close after completion stays idempotent.
	if err := serverConn.Close(); err != nil {
		t.Fatalf("redundant Close returned %v, want nil", err)
	}

	stop()
	alloc.assertBalanced(t)
	if !counters.balanced() {
		t.Fatalf("open/close mismatch: %+v", counters)
	}
	assertNoNbioGoroutines(t)
}

// fillConnSendBuffer writes through the nbio Conn until its writes are fully
// queued (EAGAIN reached), guaranteeing the next writes enter writeList.
func fillConnSendBuffer(t *testing.T, c *Conn, payload []byte) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	written := 0
	for {
		n, err := c.Write(payload)
		if err != nil {
			// Deadline/overflow would break determinism; allow EAGAIN-driven
			// queueing only (Write swallows EAGAIN and returns len, nil).
			t.Fatalf("fill write returned err=%v after %d bytes", err, written)
		}
		written += n
		if queuedN := writeListLen(c); queuedN >= 1 {
			// At least one byte is queued: kernel buffer is full.
			break
		}
		if written > 64*1024*1024 {
			t.Fatalf("could not fill send buffer")
		}
	}
	_ = c.SetWriteDeadline(time.Time{})
}

// TestLifecycleAsyncWriteThenClose: OnData triggers a write from another
// goroutine and closes immediately. Both orders (write-wins vs close-wins)
// must be safe: at most one OnClose, no panic, buffer accounting balanced,
// and every write-size callback observes a still-live buffer.
func TestLifecycleAsyncWriteThenClose(t *testing.T) {
	for _, variant := range []string{"concurrent", "writeBeforeClose", "closeBeforeWrite"} {
		t.Run(variant, func(t *testing.T) {
			testAsyncWriteThenClose(t, variant)
		})
	}
}

func testAsyncWriteThenClose(t *testing.T, variant string) {
	var (
		log      = newEventLog()
		counters lifecycleCounters
		alloc    = newTrackingAllocator()

		gotData   = make(chan struct{})
		releaseOp = make(chan struct{})
		closedCh  = make(chan struct{})
		connMu    sync.Mutex
		conn      *Conn
	)

	g, a, addr, stop := startLifecycleServer(t, 2, alloc,
		func(c *Conn) {
			connMu.Lock()
			conn = c
			connMu.Unlock()
			counters.onOpen()
			log.record("open")
		},
		func(c *Conn, data []byte) {
			log.record("data")
			// Pin both contenders until the test releases them.
			select {
			case <-gotData:
			default:
				close(gotData)
			}
			<-releaseOp

			payload := append([]byte{}, data...)
			switch variant {
			case "concurrent":
				go func() { _, _ = c.Write(payload) }()
				_ = c.Close()
			case "writeBeforeClose":
				_, _ = c.Write(payload)
				_ = c.Close()
			case "closeBeforeWrite":
				_ = c.Close()
				_, _ = c.Write(payload)
			}
		},
		func(c *Conn, err error) {
			log.record("close")
			counters.onClose()
			close(closedCh)
		},
	)
	_ = a
	defer stop()

	// Sanity: onWrittenSize must never see released memory.
	g.OnWrittenSize(func(c *Conn, b []byte, n int) {
		assertNoPoison(t, b[:n], "OnWrittenSize")
	})

	peer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = peer.Close() }()

	connMu.Lock()
	for conn == nil {
		connMu.Unlock()
		runtime.Gosched()
		connMu.Lock()
	}
	connMu.Unlock()

	if _, err := peer.Write(bigPayload[:4096]); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	waitSignal(t, gotData, "OnData entry")
	close(releaseOp)

	waitSignal(t, closedCh, "OnClose")

	// The write goroutine (if any) and the write/close pair must have settled
	// before accounting is checked; Stop drains the async list.
	stop()

	log.assertCount(t, "open", 1)
	log.assertCount(t, "data", 1)
	log.assertCount(t, "close", 1)
	alloc.assertBalanced(t)
	if !counters.balanced() {
		t.Fatalf("open/close mismatch: %+v", counters)
	}
	assertNoNbioGoroutines(t)
}

// TestLifecycleAcceptDuringStop pins the exact race: Accept returns and the
// listener has released the accepted conn, but Stop has already begun. The
// accepted connection must still end up closed exactly once and Stop must
// return.
func TestLifecycleAcceptDuringStop(t *testing.T) {
	var (
		log           = newEventLog()
		counters      lifecycleCounters
		alloc         = newTrackingAllocator()
		acceptHeld    = make(chan struct{})
		stopInWindow  = make(chan struct{})
		releaseAccept = make(chan struct{})
		onCloseCh     = make(chan struct{})
	)

	open := func(c *Conn) {
		counters.onOpen()
		log.record("open")
	}
	closeFn := func(c *Conn, err error) {
		log.record("close")
		counters.onClose()
		select {
		case <-onCloseCh:
		default:
			close(onCloseCh)
		}
	}

	g, _, addr, _ := startLifecycleServer(t, 1, alloc, open, nil, closeFn)

	// Hold the listener between Accept and addConn, inside the exact window
	// that Stop must synchronize against.
	testHookAfterAccepted = func(c *Conn) {
		close(acceptHeld)
		<-releaseAccept
	}
	testHookAfterListenersClosed = func() {
		close(stopInWindow)
	}

	// Dial first so the accept is guaranteed pending when Stop begins.
	peer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = peer.Close() }()
	waitSignal(t, acceptHeld, "accept held")

	stopReturned := make(chan struct{})
	go func() {
		g.Stop()
		close(stopReturned)
	}()

	// Release the accepted conn only after listeners have been closed inside
	// Stop; addConn/onOpen then races the snapshot and must still be tracked.
	waitSignal(t, stopInWindow, "Stop past listener shutdown")
	close(releaseAccept)

	select {
	case <-stopReturned:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("Stop deadlocked while a conn was accepted during shutdown")
	}

	// Restore hooks before any other engine activity in the process.
	testHookAfterAccepted = nil
	testHookAfterListenersClosed = nil

	waitSignal(t, onCloseCh, "OnClose for accepted conn")

	// Allow the async OnClose job to finish before accounting.
	deadline := time.Now().Add(lifecycleTestTimeout)
	for time.Now().Before(deadline) && !counters.balanced() {
		runtime.Gosched()
	}
	if !counters.balanced() {
		t.Fatalf("accepted conn leaked: open=%d close=%d",
			atomic.LoadInt64(&counters.open),
			atomic.LoadInt64(&counters.close))
	}
	alloc.assertBalanced(t)
	assertNoNbioGoroutines(t)
}

// TestLifecycleReentrantCloseInOnClose: the user OnClose callback calls Close
// again on the same conn. The close path must be idempotent and must not
// republish the close event (which would recurse or deadlock the async job
// list). OnClose must run exactly once.
func TestLifecycleReentrantCloseInOnClose(t *testing.T) {
	var (
		log      = newEventLog()
		counters lifecycleCounters
		alloc    = newTrackingAllocator()

		gotData   = make(chan struct{})
		onCloseCh = make(chan struct{})
		connMu    sync.Mutex
		conn      *Conn
	)

	open := func(c *Conn) {
		connMu.Lock()
		conn = c
		connMu.Unlock()
		counters.onOpen()
		log.record("open")
	}
	data := func(c *Conn, b []byte) {
		log.record("data")
		select {
		case <-gotData:
		default:
			close(gotData)
		}
	}
	closeFn := func(c *Conn, err error) {
		log.record("close")
		counters.onClose()
		// Re-enter the close path from within the close callback.
		if cerr := c.Close(); cerr != nil {
			t.Errorf("Close inside OnClose returned %v, want nil", cerr)
		}
		// A third call after that must still be a no-op.
		if cerr := c.Close(); cerr != nil {
			t.Errorf("second nested Close returned %v, want nil", cerr)
		}
		close(onCloseCh)
	}

	_, _, addr, stop := startLifecycleServer(t, 2, alloc, open, data, closeFn)
	defer stop()

	peer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	connMu.Lock()
	for conn == nil {
		connMu.Unlock()
		runtime.Gosched()
		connMu.Lock()
	}
	connMu.Unlock()

	if _, err := peer.Write([]byte("ping")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	waitSignal(t, gotData, "OnData")

	// Peer half-close => EOF => engine close => user OnClose re-enters Close.
	if err := peer.Close(); err != nil {
		t.Fatalf("peer close: %v", err)
	}

	waitSignal(t, onCloseCh, "OnClose")

	stop()

	log.assertCount(t, "open", 1)
	log.assertCount(t, "data", 1)
	log.assertCount(t, "close", 1)
	log.assertOrdered(t, "open", "data", "close")
	if !counters.balanced() {
		t.Fatalf("open/close mismatch: %+v", counters)
	}
	alloc.assertBalanced(t)
	assertNoNbioGoroutines(t)
}

// TestLifecycleEventOrderSameConn asserts that for one connection callbacks
// appear in order: open -> data* -> close, and no data fires after close.
// Distinct connections are not globally ordered.
func TestLifecycleEventOrderSameConn(t *testing.T) {
	var (
		log      = newEventLog()
		counters lifecycleCounters
		alloc    = newTrackingAllocator()

		gotDataN  = make(chan struct{})
		onCloseCh = make(chan struct{})

		dataN  int32
		connMu sync.Mutex
		conn   *Conn
	)

	_, _, addr, stop := startLifecycleServer(t, 3, alloc,
		func(c *Conn) {
			connMu.Lock()
			conn = c
			connMu.Unlock()
			counters.onOpen()
			log.record("open")
		},
		func(c *Conn, b []byte) {
			n := atomic.AddInt32(&dataN, 1)
			log.record("data")
			if n == 3 {
				close(gotDataN)
			}
		},
		func(c *Conn, err error) {
			log.record("close")
			counters.onClose()
			close(onCloseCh)
		},
	)
	defer stop()

	peer, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	connMu.Lock()
	for conn == nil {
		connMu.Unlock()
		runtime.Gosched()
		connMu.Lock()
	}
	connMu.Unlock()

	for i := 0; i < 3; i++ {
		if _, err := peer.Write([]byte("x")); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	waitSignal(t, gotDataN, "3 data events")

	_ = conn.Close()
	waitSignal(t, onCloseCh, "OnClose")

	// The conn was closed from the engine side; keep the peer tidy.
	_ = peer.Close()

	stop()

	log.assertCount(t, "open", 1)
	log.assertCount(t, "close", 1)
	if n := atomic.LoadInt32(&dataN); n != 3 {
		t.Fatalf("data events = %d, want 3", n)
	}
	log.assertOrdered(t, "open", "data", "data", "data", "close")
	alloc.assertBalanced(t)
	assertNoNbioGoroutines(t)
}
