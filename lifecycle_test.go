// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Deterministic lifecycle tests. They pin down fixed interleavings with
// channels, barriers and the test-only hooks in test_hooks.go instead of
// relying on random stress scheduling.

const (
	lifecycleWait     = 3 * time.Second
	lifecycleNegative = 150 * time.Millisecond
	lifecyclePoison   = 0xDD
)

// ---- poisoned allocator: detects double free and write-after-free ----

type testAllocator struct {
	mu sync.Mutex
	// live holds the *[]byte keys nbio has malloced and not freed yet.
	live map[*[]byte]struct{}
	nm   int64
	nf   int64
}

func newTestAllocator() *testAllocator {
	return &testAllocator{live: map[*[]byte]struct{}{}}
}

func (a *testAllocator) Malloc(size int) *[]byte {
	b := make([]byte, size)
	a.mu.Lock()
	if _, ok := a.live[&b]; ok {
		panic("malloc returned a pointer still marked live")
	}
	a.live[&b] = struct{}{}
	a.nm++
	a.mu.Unlock()
	return &b
}

func (a *testAllocator) Free(p *[]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p == nil {
		panic("free nil buffer")
	}
	if _, ok := a.live[p]; !ok {
		panic("free of non-owned or already-freed buffer")
	}
	delete(a.live, p)
	a.nf++
	for i := range *p {
		(*p)[i] = lifecyclePoison
	}
}

func (a *testAllocator) Realloc(p *[]byte, size int) *[]byte {
	if cap(*p) >= size {
		*p = (*p)[:size]
		return p
	}
	np := a.Malloc(size)
	copy(*np, *p)
	a.Free(p)
	return np
}

func (a *testAllocator) Append(p *[]byte, more ...byte) *[]byte {
	if cap(*p)-len(*p) >= len(more) {
		np := append(*p, more...)
		return &np
	}
	np := a.Malloc(len(*p) + len(more))
	copy(*np, *p)
	copy((*np)[len(*p):], more)
	a.Free(p)
	return np
}

func (a *testAllocator) AppendString(p *[]byte, s string) *[]byte {
	return a.Append(p, []byte(s)...)
}

func (a *testAllocator) stats() (malloc, free, live int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nm, a.nf, int64(len(a.live))
}

// ---- per connection event record ----

type connRecord struct {
	opens  int64
	closes int64
	mu     sync.Mutex
	errs   []error
	order  []string
}

func newConnRecord() *connRecord { return &connRecord{} }

func (r *connRecord) addOpen() {
	atomic.AddInt64(&r.opens, 1)
	r.mu.Lock()
	r.order = append(r.order, "open")
	r.mu.Unlock()
}

func (r *connRecord) addClose(err error) {
	atomic.AddInt64(&r.closes, 1)
	r.mu.Lock()
	r.order = append(r.order, "close")
	r.errs = append(r.errs, err)
	r.mu.Unlock()
}

func (r *connRecord) counts() (int, int) {
	return int(atomic.LoadInt64(&r.opens)), int(atomic.LoadInt64(&r.closes))
}

func (r *connRecord) ordered() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	seenClose := false
	for _, e := range r.order {
		if e == "open" && seenClose {
			return false
		}
		if e == "close" {
			seenClose = true
		}
	}
	return true
}

func (r *connRecord) lastErr() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.errs) == 0 {
		return nil
	}
	return r.errs[len(r.errs)-1]
}

// ---- test harness ----

type lifecycleHarness struct {
	t         *testing.T
	g         *Engine
	alloc     *testAllocator
	npoller   int
	listenFn  func(network, addr string) (net.Listener, error)
	hooks     *testHooks
	onOpen    func(c *Conn)
	onClose   func(c *Conn, err error)
	onData    func(c *Conn, data []byte)
	openCh    chan *Conn
	closeCh   chan *Conn
	openMu    sync.Mutex
	openByKey map[string]*Conn
	records   sync.Map
}

func newHarness(t *testing.T) *lifecycleHarness {
	return &lifecycleHarness{
		t:         t,
		alloc:     newTestAllocator(),
		npoller:   1,
		openCh:    make(chan *Conn, 256),
		closeCh:   make(chan *Conn, 256),
		openByKey: map[string]*Conn{},
	}
}

func (h *lifecycleHarness) recordFor(c *Conn) *connRecord {
	if v, ok := h.records.Load(c); ok {
		return v.(*connRecord)
	}
	r := newConnRecord()
	actual, _ := h.records.LoadOrStore(c, r)
	return actual.(*connRecord)
}

func freeListenAddr(t *testing.T) (string, net.Listener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve listener: %v", err)
	}
	return ln.Addr().String(), ln
}

func (h *lifecycleHarness) start() {
	h.t.Helper()
	addr, reserved := freeListenAddr(h.t)
	ln := net.Listener(reserved)
	if h.listenFn != nil {
		var err error
		ln, err = h.listenFn("tcp", addr)
		if err != nil {
			h.t.Fatalf("custom listener: %v", err)
		}
	}

	g := NewEngine(Config{
		Network:       "tcp",
		Addrs:         []string{addr},
		NPoller:       h.npoller,
		BodyAllocator: h.alloc,
		Listen: func(network, a string) (net.Listener, error) {
			if a == addr {
				return ln, nil
			}
			return net.Listen(network, a)
		},
	})
	h.g = g
	if h.hooks != nil {
		g.testHooks = h.hooks
	}
	g.OnOpen(func(c *Conn) {
		h.recordFor(c).addOpen()
		h.openMu.Lock()
		if c.RemoteAddr() != nil {
			h.openByKey[c.RemoteAddr().String()] = c
		}
		h.openMu.Unlock()
		h.openCh <- c
		if h.onOpen != nil {
			h.onOpen(c)
		}
	})
	g.OnClose(func(c *Conn, err error) {
		h.recordFor(c).addClose(err)
		if h.onClose != nil {
			h.onClose(c, err)
		}
		h.closeCh <- c
	})
	if h.onData != nil {
		g.OnData(h.onData)
	}
	if err := g.Start(); err != nil {
		h.t.Fatalf("start: %v", err)
	}
}

func (h *lifecycleHarness) dialClient() (net.Conn, *Conn) {
	h.t.Helper()
	raw, err := net.Dial("tcp", h.g.Addrs[0])
	if err != nil {
		h.t.Fatalf("dial: %v", err)
	}
	nbc, err := h.g.AddConn(raw)
	if err != nil {
		_ = raw.Close()
		h.t.Fatalf("addconn: %v", err)
	}
	return raw, nbc
}

// dialServer returns the raw peer and the nbio Conn accepted by the engine.
func (h *lifecycleHarness) dialServer() (net.Conn, *Conn) {
	h.t.Helper()
	raw, err := net.Dial("tcp", h.g.Addrs[0])
	if err != nil {
		h.t.Fatalf("dial: %v", err)
	}
	server := h.waitOpenMatching(raw.LocalAddr().String())
	return raw, server
}

func (h *lifecycleHarness) waitOpen() *Conn {
	h.t.Helper()
	select {
	case c := <-h.openCh:
		return c
	case <-time.After(lifecycleWait):
		h.t.Fatalf("timeout waiting OnOpen")
	}
	return nil
}

func (h *lifecycleHarness) waitOpenMatching(raddr string) *Conn {
	h.t.Helper()
	deadline := time.After(lifecycleWait)
	for {
		select {
		case <-h.openCh:
			h.openMu.Lock()
			c := h.openByKey[raddr]
			h.openMu.Unlock()
			if c != nil {
				return c
			}
		case <-deadline:
			h.t.Fatalf("timeout waiting OnOpen for %v", raddr)
		}
	}
}

func (h *lifecycleHarness) waitClose(c *Conn) error {
	h.t.Helper()
	deadline := time.After(lifecycleWait)
	for {
		select {
		case got := <-h.closeCh:
			if got == c {
				return h.recordFor(c).lastErr()
			}
		case <-deadline:
			h.t.Fatalf("timeout waiting OnClose")
		}
	}
}

func (h *lifecycleHarness) assertSingleClose(c *Conn) {
	h.t.Helper()
	deadline := time.After(lifecycleWait)
	for {
		select {
		case got := <-h.closeCh:
			if got == c {
				goto asserted
			}
		case <-deadline:
			h.t.Fatalf("timeout waiting OnClose")
		}
	}
asserted:
	select {
	case got := <-h.closeCh:
		if got == c {
			h.t.Fatalf("duplicate OnClose event")
		}
	case <-time.After(lifecycleNegative):
	}
	if o, cl := h.recordFor(c).counts(); o != 1 || cl != 1 {
		h.t.Fatalf("callback counts: opens=%v closes=%v, want 1/1", o, cl)
	}
}

func (h *lifecycleHarness) assertBalanced() {
	if m, f, l := h.alloc.stats(); m != f || l != 0 {
		h.t.Fatalf("write allocator unbalanced: malloc=%v free=%v live=%v", m, f, l)
	}
}

func (h *lifecycleHarness) assertEventsOrdered() {
	h.records.Range(func(k, v interface{}) bool {
		c := k.(*Conn)
		r := v.(*connRecord)
		if o, cl := r.counts(); o != 1 || cl != 1 {
			h.t.Fatalf("conn %p callback counts: opens=%v closes=%v, want 1/1", c, o, cl)
		}
		if !r.ordered() {
			h.t.Fatalf("conn %p events out of order", c)
		}
		return true
	})
}

// stopAndClean asserts Stop returns promptly, callbacks fire exactly once in
// order, the write allocator balances, and nbio goroutines exit.
func (h *lifecycleHarness) stopAndClean() {
	h.t.Helper()
	done := make(chan struct{})
	go func() {
		h.g.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(lifecycleWait):
		h.t.Fatalf("Engine.Stop hung")
	}
	h.assertEventsOrdered()
	h.assertBalanced()
	h.waitGoroutinesGone()
}

// nbioGoroutineCount counts goroutines executing nbio engine/poller code.
func nbioGoroutineCount() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	count := 0
	for _, st := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(st, "github.com/lesismal/nbio.") {
			continue
		}
		if strings.Contains(st, "acceptorLoop") ||
			strings.Contains(st, "readWriteLoop") ||
			strings.Contains(st, "(*poller).readConn") ||
			strings.Contains(st, "timer.(*Timer).Async") {
			count++
		}
	}
	return count
}

func (h *lifecycleHarness) waitGoroutinesGone() {
	h.t.Helper()
	deadline := time.Now().Add(lifecycleWait)
	var last int
	for time.Now().Before(deadline) {
		last = nbioGoroutineCount()
		if last == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	b := make([]byte, 1<<20)
	n := runtime.Stack(b, true)
	var out []string
	for _, st := range strings.Split(string(b[:n]), "\n\n") {
		if strings.Contains(st, "github.com/lesismal/nbio.") {
			out = append(out, st)
		}
	}
	h.t.Fatalf("nbio goroutines remain after Stop: %v\n%v", last, strings.Join(out, "\n---\n"))
}

func assertCloseErrCategory(t *testing.T, err error) {
	t.Helper()
	switch {
	case err == nil, errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		return
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "reset") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "closed") ||
		strings.Contains(msg, "forcibly") {
		return
	}
	t.Fatalf("unexpected close error category: %T %v", err, err)
}

// ---- barrier listener: pins an accept to the middle of Stop ----
//
// The first Accept that returns a conn parks: it tells the test a conn is
// ready, waits for the test to signal "Stop is closing listeners", and only
// then returns the conn to addConn. Listener.Close blocks until that
// parked Accept has returned, so the backing listener stays alive while the
// accepted conn is registered during the Stop window.
type barrierListener struct {
	net.Listener

	mu          sync.Mutex
	accepted    bool // first Accept has returned a conn
	released    bool // test allowed the parked Accept to return
	closing     bool
	acceptReady chan struct{} // closed when the first conn is accepted & parked
	doRelease   chan struct{} // closed by the test to release the park
	connHanded  chan struct{} // closed when the parked conn is returned to addConn
}

func newBarrierListener(ln net.Listener) *barrierListener {
	return &barrierListener{
		Listener:    ln,
		acceptReady: make(chan struct{}),
		doRelease:   make(chan struct{}),
		connHanded:  make(chan struct{}),
	}
}

func (l *barrierListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return conn, err
	}
	l.mu.Lock()
	if !l.accepted {
		l.accepted = true
		l.mu.Unlock()

		close(l.acceptReady)
		<-l.doRelease
		close(l.connHanded)
		return conn, nil
	}
	l.mu.Unlock()
	return conn, nil
}

func (l *barrierListener) releaseParkedAccept() {
	close(l.doRelease)
}

func (l *barrierListener) Close() error {
	l.mu.Lock()
	l.closing = true
	accepted := l.accepted
	l.mu.Unlock()
	if accepted {
		// Do not close the backing listener until the parked Accept has
		// handed its conn to addConn.
		<-l.connHanded
	}
	return l.Listener.Close()
}

// fixedSequence returns a deterministic LCG stream; the exact sequence is
// part of the test contract and never depends on time or scheduling.
func fixedSequence(seed uint64) func() uint64 {
	x := seed*6364136223846793005 + 1442695040888963407
	return func() uint64 {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		return x
	}
}
