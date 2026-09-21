// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"io"
	"net"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/nbio/mempool"
)

// lifecycleStats records the callback observations a lifecycle test asserts
// on. Events of a single connection must be delivered in open -> data* ->
// close order, and close must be reported exactly once. Events belonging to
// different connections may interleave freely.
type lifecycleStats struct {
	mu sync.Mutex

	opens  map[uint64]int
	closes map[uint64]int
	datas  map[uint64]int

	closeErrs map[uint64]error
	order     []uint64

	openCh  chan *Conn
	closeCh chan connClose
}

type connClose struct {
	c   *Conn
	err error
}

// connID is stored as the Conn session so test bookkeeping uses stable ids
// instead of relying on pointer values.
type connID struct {
	id uint64
}

func connEventID(c *Conn, ev int) uint64 {
	id := uint64(0)
	if v, ok := c.Session().(*connID); ok {
		id = v.id
	}
	return id<<2 | uint64(ev)
}

func newLifecycleStats() *lifecycleStats {
	return &lifecycleStats{
		opens:     map[uint64]int{},
		closes:    map[uint64]int{},
		datas:     map[uint64]int{},
		closeErrs: map[uint64]error{},
		openCh:    make(chan *Conn, 64),
		closeCh:   make(chan connClose, 64),
	}
}

func (s *lifecycleStats) onOpen(c *Conn) {
	s.mu.Lock()
	s.opens[connEventID(c, 1)>>2]++
	s.order = append(s.order, connEventID(c, 1))
	s.mu.Unlock()
	select {
	case s.openCh <- c:
	default:
	}
}

func (s *lifecycleStats) onData(c *Conn) {
	s.mu.Lock()
	s.datas[connEventID(c, 2)>>2]++
	s.order = append(s.order, connEventID(c, 2))
	s.mu.Unlock()
}

func (s *lifecycleStats) onClose(c *Conn, err error) {
	s.mu.Lock()
	id := connEventID(c, 3) >> 2
	s.closes[id]++
	s.closeErrs[id] = err
	s.order = append(s.order, connEventID(c, 3))
	s.mu.Unlock()
	select {
	case s.closeCh <- connClose{c, err}:
	default:
	}
}

func (s *lifecycleStats) idOf(c *Conn) uint64 {
	if v, ok := c.Session().(*connID); ok {
		return v.id
	}
	return 0
}

func (s *lifecycleStats) waitClose(t *testing.T, who string) connClose {
	t.Helper()
	select {
	case cc := <-s.closeCh:
		return cc
	case <-time.After(time.Second * 5):
		t.Fatalf("%s: timeout waiting for OnClose", who)
		return connClose{}
	}
}

func (s *lifecycleStats) closesOf(c *Conn) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes[s.idOf(c)]
}

// sameConnOrdered verifies that for every connection the recorded events are
// in open(1) -> data(2)* -> close(3) order with a single terminal event.
func (s *lifecycleStats) sameConnOrdered(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	stage := map[uint64]int{}
	for _, tag := range s.order {
		id := tag >> 2
		ev := int(tag & 3)
		last := stage[id]
		switch ev {
		case 1:
			if last != 0 {
				t.Fatalf("conn %v: duplicate/reordered OnOpen, stage %v", id, last)
			}
		case 2:
			if last != 1 && last != 2 {
				t.Fatalf("conn %v: OnData out of order, stage %v", id, last)
			}
		case 3:
			if last == 0 || last == 3 {
				t.Fatalf("conn %v: OnClose out of order/duplicated, stage %v", id, last)
			}
		}
		stage[id] = ev
	}
}

// gate is a one-to-one rendezvous used to pin an engine/poller goroutine at
// a fixed execution phase while the test drives another goroutine.
type gate struct {
	onceArrive sync.Once
	onceGo     sync.Once
	arrived    chan struct{}
	release    chan struct{}
}

func newGate() *gate {
	return &gate{
		arrived: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (g *gate) waitArrived(t *testing.T, who string) {
	t.Helper()
	select {
	case <-g.arrived:
	case <-time.After(time.Second * 5):
		t.Fatalf("%s: timeout waiting for gate arrival", who)
	}
}

// signal marks the gate as arrived (first call wins) without blocking.
func (g *gate) signal() {
	g.onceArrive.Do(func() { close(g.arrived) })
}

// hold signals arrival and blocks until letGo, pinning an engine/poller
// goroutine at a fixed phase.
func (g *gate) hold() {
	g.signal()
	<-g.release
}

func (g *gate) letGo() {
	g.onceGo.Do(func() { close(g.release) })
}

// lifecycleEngine builds a single-poller TCP engine on an ephemeral loopback
// port with the given stats and allocator, and starts it.
func lifecycleEngine(t *testing.T, st *lifecycleStats, alloc mempool.Allocator) *Engine {
	t.Helper()
	return startEngine(t, lifecycleEngineHooks(t, st, alloc, nil, nil))
}

// pollerBaseline is captured before a test starts its engine.
func pollerBaseline() int { return countPollerGoroutines() }

// lifecycleEngineConfigured builds a single-poller loopback engine whose
// tests may install package-level test hooks inside configure; configure
// runs before Start so no engine goroutine can observe a partial hook.
func lifecycleEngineConfigured(
	t *testing.T,
	st *lifecycleStats,
	alloc mempool.Allocator,
	onOpenExtra func(c *Conn),
	onDataExtra func(c *Conn, data []byte),
	configure func(g *Engine),
) *Engine {
	t.Helper()
	g := lifecycleEngineHooks(t, st, alloc, onOpenExtra, onDataExtra)
	if configure != nil {
		configure(g)
	}
	return startEngine(t, g)
}

func lifecycleEngineHooks(
	t *testing.T,
	st *lifecycleStats,
	alloc mempool.Allocator,
	onOpenExtra func(c *Conn),
	onDataExtra func(c *Conn, data []byte),
) *Engine {
	t.Helper()
	g := NewEngine(Config{
		Name:          "lifecycle",
		Network:       "tcp",
		Addrs:         []string{"127.0.0.1:0"},
		NPoller:       1,
		BodyAllocator: alloc,
	})
	var idSeq uint64
	g.OnOpen(func(c *Conn) {
		id := atomic.AddUint64(&idSeq, 1)
		c.SetSession(&connID{id: id})
		st.onOpen(c)
		if onOpenExtra != nil {
			onOpenExtra(c)
		}
	})
	g.OnData(func(c *Conn, data []byte) {
		st.onData(c)
		if onDataExtra != nil {
			onDataExtra(c, data)
		}
	})
	g.OnClose(func(c *Conn, err error) {
		st.onClose(c, err)
	})
	return g
}

// startEngine starts the engine built by the lifecycle helpers.
func startEngine(t *testing.T, g *Engine) *Engine {
	t.Helper()
	if err := g.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	return g
}

func dialRaw(t *testing.T, addr string) *net.TCPConn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return conn.(*net.TCPConn)
}

// stopEngine runs Engine.Stop and fails if it does not return promptly: it
// must wake every poller and wait for every tracked connection callback.
func stopEngine(t *testing.T, g *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second * 10):
		_ = pprof.Lookup("goroutine").WriteTo(testWriter{t}, 2)
		t.Fatalf("Engine.Stop deadlocked")
	}
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

// countPollerGoroutines returns how many live goroutines run nbio poller
// loops. Tests snapshot the baseline before starting their engine so pollers
// belonging to other engines (e.g. the package level echo engine) do not
// count as leaks.
func countPollerGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	text := string(buf[:n])
	return countOccurrences(text, "nbio.(*poller).readWriteLoop") +
		countOccurrences(text, "nbio.(*poller).acceptorLoop")
}

func countOccurrences(s, sub string) int {
	count := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			count++
		}
	}
	return count
}

// noLeakedGoroutines requires the nbio poller/listener goroutines added after
// the baseline snapshot to drain after the engine stops.
func noLeakedGoroutines(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(time.Second * 2)
	for {
		if countPollerGoroutines() <= baseline {
			return
		}
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("nbio poller goroutines leaked after Stop (baseline %v):\n%v",
				baseline, string(buf[:n]))
		}
		time.Sleep(time.Millisecond * 20)
	}
}

// isPeerCloseErr classifies the terminal error delivered to OnClose when the
// peer closes: io.EOF (ordered shutdown) or the reset/broken-pipe errors the
// kernel reports when queued data is lost.
func isPeerCloseErr(err error) bool {
	if err == nil || errors.Is(err, io.EOF) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return false
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNABORTED) {
		return true
	}
	return false
}

// trackingAllocator wraps a mempool.Allocator and records every live buffer;
// a double Free or a missing Free at test end fails deterministically.
type trackingAllocator struct {
	inner mempool.Allocator

	mu     sync.Mutex
	live   map[string]int
	allocN int64
	freeN  int64
}

func newTrackingAllocator(inner mempool.Allocator) *trackingAllocator {
	return &trackingAllocator{inner: inner, live: map[string]int{}}
}

func bufferKey(buf *[]byte) string {
	if buf == nil || cap(*buf) == 0 {
		return ""
	}
	ptr := (*[2]uintptr)(unsafe.Pointer(buf))[0]
	return string(uintptrBytes(ptr, cap(*buf)))
}

func uintptrBytes(ptr uintptr, n int) []byte {
	b := make([]byte, 8+8)
	*(*uintptr)(unsafe.Pointer(&b[0])) = ptr
	*(*uintptr)(unsafe.Pointer(&b[8])) = uintptr(n)
	return b
}

func (a *trackingAllocator) track(buf *[]byte, deltaAlloc int) {
	if k := bufferKey(buf); k != "" {
		a.live[k] += deltaAlloc
		if a.live[k] == 0 {
			delete(a.live, k)
		}
	}
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	buf := a.inner.Malloc(size)
	a.mu.Lock()
	a.allocN++
	a.track(buf, 1)
	a.mu.Unlock()
	return buf
}

func (a *trackingAllocator) Realloc(buf *[]byte, size int) *[]byte {
	nb := a.inner.Realloc(buf, size)
	a.mu.Lock()
	a.freeN++
	a.track(buf, -1)
	a.allocN++
	a.track(nb, 1)
	a.mu.Unlock()
	return nb
}

func (a *trackingAllocator) Append(buf *[]byte, more ...byte) *[]byte {
	nb := a.inner.Append(buf, more...)
	a.mu.Lock()
	a.freeN++
	a.track(buf, -1)
	a.allocN++
	a.track(nb, 1)
	a.mu.Unlock()
	return nb
}

func (a *trackingAllocator) AppendString(buf *[]byte, more string) *[]byte {
	nb := a.inner.AppendString(buf, more)
	a.mu.Lock()
	a.freeN++
	a.track(buf, -1)
	a.allocN++
	a.track(nb, 1)
	a.mu.Unlock()
	return nb
}

func (a *trackingAllocator) Free(buf *[]byte) {
	a.mu.Lock()
	k := bufferKey(buf)
	if a.live[k] <= 0 {
		a.mu.Unlock()
		panic("trackingAllocator: double free or free of unallocated buffer")
	}
	a.freeN++
	a.track(buf, -1)
	a.mu.Unlock()
	a.inner.Free(buf)
}

func (a *trackingAllocator) counts() (allocN, freeN int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.allocN, a.freeN
}

func (a *trackingAllocator) assertEmpty(t *testing.T) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.live) != 0 || a.allocN != a.freeN {
		t.Fatalf("write buffers not returned exactly once: live=%v alloc=%v free=%v",
			len(a.live), a.allocN, a.freeN)
	}
}
