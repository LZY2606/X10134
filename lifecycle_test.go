// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/nbio/mempool"
)

// lifecycleTestTimeout bounds every wait on a callback/goroutine. All
// interleavings are driven explicitly by channels, so this is only a
// deadlock watchdog, never a scheduling delay.
const lifecycleTestTimeout = 5 * time.Second

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timeout waiting for %s", what)
	}
}

func stopEngine(t *testing.T, g *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("Engine.Stop deadlocked")
	}
}

// assertNoNbioGoroutines fails if poller/listener goroutines owned by
// the given engine are still alive after its Stop. Matching the poller
// receiver pointers in stacks keeps this independent of other engines
// that may coexist in the same test binary (e.g. legacy tests whose
// package-global engine lives until their own last test).
func assertNoNbioGoroutines(t *testing.T, g *Engine) {
	t.Helper()
	want := map[string]bool{}
	for _, p := range g.pollers {
		want[fmt.Sprintf("(*poller).start(0x%x)", uintptr(unsafe.Pointer(p)))] = true
		want[fmt.Sprintf("(*poller).readWriteLoop(0x%x)", uintptr(unsafe.Pointer(p)))] = true
		want[fmt.Sprintf("(*poller).acceptorLoop(0x%x)", uintptr(unsafe.Pointer(p)))] = true
	}
	for _, l := range g.listeners {
		want[fmt.Sprintf("(*poller).start(0x%x)", uintptr(unsafe.Pointer(l)))] = true
		want[fmt.Sprintf("(*poller).acceptorLoop(0x%x)", uintptr(unsafe.Pointer(l)))] = true
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		leaked := false
		for _, st := range strings.Split(string(buf[:n]), "\n\n") {
			if !strings.Contains(st, "github.com/lesismal/nbio") ||
				strings.Contains(st, "_test.go") {
				continue
			}
			for marker := range want {
				if strings.Contains(st, marker) {
					leaked = true
					break
				}
			}
		}
		if !leaked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("engine poller goroutines still alive after Stop")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// trackingAllocator wraps the default allocator and verifies that every
// Malloc'd write buffer is Freed exactly once. Free poison-fills the
// memory so a write after release produces a deterministic corrupt
// pattern that the flushed-bytes hook can assert against (instead of
// relying on -race alone).
type liveRange struct {
	base, end uintptr
}

type trackingAllocator struct {
	inner mempool.Allocator

	mu     sync.Mutex
	lived  []liveRange // backing ranges currently checked out
	malloc int64
	free   int64
	double int64

	debugMu     sync.Mutex
	debugRanges map[uintptr]string
}

const trackingPoisonByte = 0xEE

func newTrackingAllocator() *trackingAllocator {
	return &trackingAllocator{
		inner: mempool.New(1024, 1024*1024*1024),
	}
}

func sliceBase(b []byte) (uintptr, uintptr) {
	if cap(b) == 0 {
		return 0, 0
	}
	back := b[:cap(b)]
	base := uintptr(unsafe.Pointer(&back[0]))
	return base, base + uintptr(cap(b))
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	if size <= 0 {
		return a.inner.Malloc(size)
	}
	atomic.AddInt64(&a.malloc, 1)
	b := a.inner.Malloc(size)
	if b != nil {
		// Pooled buffers may retain poison bytes left by a previous
		// Free (including beyond len up to cap); zero the full backing
		// array so only an actual release-then-write looks poisoned.
		full := (*b)[:cap(*b)]
		for i := range full {
			full[i] = 0
		}
		base, end := sliceBase(*b)
		a.mu.Lock()
		a.lived = append(a.lived, liveRange{base, end})
		a.mu.Unlock()
	}
	return b
}

func (a *trackingAllocator) Realloc(b *[]byte, size int) *[]byte {
	if b == nil {
		return a.Malloc(size)
	}
	if cap(*b) >= size {
		*b = (*b)[:size]
		return b
	}
	nb := a.Malloc(size)
	copy(*nb, *b)
	a.Free(b)
	return nb
}

func (a *trackingAllocator) Append(b *[]byte, more ...byte) *[]byte {
	if b == nil {
		return a.Malloc(len(more))
	}
	// Always grow through the zero-checked Malloc instead of the inner
	// pool's in-place append, whose reused backing may expose stale
	// poison bytes in the cap-len tail.
	nb := a.Malloc(len(*b) + len(more))
	copy(*nb, *b)
	copy((*nb)[len(*b):], more)
	a.Free(b)
	return nb
}

func (a *trackingAllocator) AppendString(b *[]byte, more string) *[]byte {
	if b == nil {
		return a.Malloc(len(more))
	}
	nb := a.Malloc(len(*b) + len(more))
	copy(*nb, *b)
	copy((*nb)[len(*b):], more)
	a.Free(b)
	return nb
}

func (a *trackingAllocator) Free(b *[]byte) {
	if b == nil || cap(*b) == 0 {
		return
	}
	base, end := sliceBase(*b)
	a.mu.Lock()
	idx := -1
	for i, r := range a.lived {
		if r.base == base && r.end == end {
			idx = i
			break
		}
	}
	if idx < 0 {
		a.mu.Unlock()
		atomic.AddInt64(&a.double, 1)
		return
	}
	a.lived[idx] = a.lived[len(a.lived)-1]
	a.lived = a.lived[:len(a.lived)-1]
	a.mu.Unlock()

	for i := 0; i < cap(*b); i++ {
		(*b)[:cap(*b)][i] = trackingPoisonByte
	}
	atomic.AddInt64(&a.free, 1)
	a.inner.Free(b)
}

// assertCachedBytesValid fails if bytes reported as just written belong
// to a buffer that has already been released (poison-filled), i.e. the
// cache reused the buffer before/during the write callback. Buffers not
// owned by this allocator (direct caller buffers) are out of scope.
func (a *trackingAllocator) assertCachedBytesValid(t *testing.T, b []byte, n int) {
	t.Helper()
	if n <= 0 || len(b) == 0 {
		return
	}
	if n > len(b) {
		n = len(b)
	}
	base, end := sliceBase(b)
	a.mu.Lock()
	tracked := false
	for _, r := range a.lived {
		if r.base <= base && end <= r.end {
			tracked = true
			break
		}
	}
	a.mu.Unlock()
	if !tracked {
		return
	}
	for i := 0; i < n; i++ {
		if b[i] == trackingPoisonByte {
			t.Fatalf("write callback observed released/reused buffer at byte %d", i)
		}
	}
}

func (a *trackingAllocator) assertBalanced(t *testing.T) {
	t.Helper()
	a.mu.Lock()
	n := len(a.lived)
	a.mu.Unlock()
	if n != 0 {
		t.Fatalf("write buffer leak: %d buffer(s) malloc'd but not freed", n)
	}
	if d := atomic.LoadInt64(&a.double); d != 0 {
		t.Fatalf("%d double free(s) of write buffers", d)
	}
	if m, f := atomic.LoadInt64(&a.malloc), atomic.LoadInt64(&a.free); m != f {
		t.Fatalf("write buffer allocations unbalanced: malloc=%d free=%d", m, f)
	}
}

const (
	evtOpen = iota + 1
	evtData
	evtClose
)

type connEvent struct {
	fd   int
	kind int
	pos  int
}

// connRecorder records callback multiplicities and per-conn ordering.
type connRecorder struct {
	mu       sync.Mutex
	opens    map[*Conn]int
	closes   map[*Conn]int
	closeErr map[*Conn]error
	order    []connEvent
	seq      int

	openCh chan *Conn
}

func newConnRecorder() *connRecorder {
	return &connRecorder{
		opens:    map[*Conn]int{},
		closes:   map[*Conn]int{},
		closeErr: map[*Conn]error{},
		openCh:   make(chan *Conn, 128),
	}
}

func (r *connRecorder) record(c *Conn, kind int) {
	r.mu.Lock()
	r.seq++
	r.order = append(r.order, connEvent{connID(c), kind, r.seq})
	r.mu.Unlock()
}

func (r *connRecorder) onOpen(c *Conn) {
	r.mu.Lock()
	r.opens[c]++
	r.seq++
	r.order = append(r.order, connEvent{connID(c), evtOpen, r.seq})
	r.mu.Unlock()
	select {
	case r.openCh <- c:
	default:
	}
}

func (r *connRecorder) onData(c *Conn, _ []byte) {
	r.record(c, evtData)
}

func (r *connRecorder) onClose(c *Conn, err error) {
	r.mu.Lock()
	r.closes[c]++
	r.closeErr[c] = err
	r.seq++
	r.order = append(r.order, connEvent{connID(c), evtClose, r.seq})
	r.mu.Unlock()
}

func (r *connRecorder) waitOpen(t *testing.T) *Conn {
	t.Helper()
	select {
	case c := <-r.openCh:
		return c
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timeout waiting for OnOpen")
		return nil
	}
}

func (r *connRecorder) waitClose(t *testing.T, target *Conn) error {
	t.Helper()
	deadline := time.Now().Add(lifecycleTestTimeout)
	for time.Now().Before(deadline) {
		r.mu.Lock()
		err := r.closeErr[target]
		n := r.closes[target]
		r.mu.Unlock()
		if n == 1 {
			return err
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for conn id=%d OnClose", connID(target))
	return nil
}

// assertOncePerConn verifies onOpen/onClose each fire exactly once per
// connection and, for each connection, open precedes data and close.
func (r *connRecorder) assertOncePerConn(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.opens) != len(r.closes) {
		t.Fatalf("open/close mismatch: %d opens vs %d closes", len(r.opens), len(r.closes))
	}
	for c, n := range r.opens {
		if n != 1 {
			t.Fatalf("id=%d OnOpen fired %d times", connID(c), n)
		}
	}
	for c, n := range r.closes {
		if n != 1 {
			t.Fatalf("id=%d OnClose fired %d times", connID(c), n)
		}
		if _, ok := r.opens[c]; !ok {
			t.Fatalf("id=%d OnClose without OnOpen", connID(c))
		}
	}
	openPos := map[int]int{}
	closePos := map[int]int{}
	dataPos := map[int]int{}
	for _, e := range r.order {
		switch e.kind {
		case evtOpen:
			if _, ok := openPos[e.fd]; !ok {
				openPos[e.fd] = e.pos
			}
		case evtData:
			if _, ok := dataPos[e.fd]; !ok {
				dataPos[e.fd] = e.pos
			}
		case evtClose:
			if _, ok := closePos[e.fd]; !ok {
				closePos[e.fd] = e.pos
			}
		}
	}
	for fd := range openPos {
		cp := closePos[fd]
		if !(openPos[fd] < cp) {
			t.Fatalf("fd=%d close event not after open event", fd)
		}
		if dp, ok := dataPos[fd]; ok && !(openPos[fd] < dp && dp < cp) {
			t.Fatalf("fd=%d data event not between open and close", fd)
		}
	}
}

// lifecycleHooks wires the recorder into a fresh listening engine on an
// ephemeral loopback port. The optional callbacks run after recording.
type lifecycleHooks struct {
	open  func(c *Conn)
	data  func(c *Conn, b []byte)
	close func(c *Conn, err error)
}

func newLifecycleEngine(t *testing.T, nPoller int, alloc mempool.Allocator, h lifecycleHooks) (*Engine, string, *connRecorder) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	conf := Config{Network: "tcp", Addrs: []string{addr}}
	if nPoller > 0 {
		conf.NPoller = nPoller
	}
	if alloc != nil {
		conf.BodyAllocator = alloc
	}
	g := NewEngine(conf)
	rec := newConnRecorder()
	g.OnOpen(func(c *Conn) {
		rec.onOpen(c)
		if h.open != nil {
			h.open(c)
		}
	})
	g.OnData(func(c *Conn, data []byte) {
		rec.onData(c, data)
		if h.data != nil {
			h.data(c, data)
		}
	})
	g.OnClose(func(c *Conn, err error) {
		rec.onClose(c, err)
		if h.close != nil {
			h.close(c, err)
		}
	})
	if err := g.Start(); err != nil {
		t.Fatalf("engine start: %v", err)
	}
	return g, addr, rec
}

func mustDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}
