// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// This file adds deterministic lifecycle tests. Every interleaving is pinned
// with channels, barriers or the test-only hooks in hooks_unix.go; sleeps are
// only used as failure guards (the success path never waits on them). All
// traffic stays on 127.0.0.1.

const lifecycleWait = 3 * time.Second

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(lifecycleWait)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}

func mustNotReceive(t *testing.T, ch interface{}, msg string) {
	t.Helper()
	select {
	case <-asSignal(ch):
		t.Fatalf("%s: event fired but should not have", msg)
	case <-time.After(50 * time.Millisecond):
	}
}

// asSignal adapts the test channels used by the lifecycle tests to one
// receive-only channel for mustNotReceive.
func asSignal(ch interface{}) <-chan struct{} {
	switch v := ch.(type) {
	case chan struct{}:
		return v
	case chan error:
		out := make(chan struct{}, 1)
		go func() {
			if err, ok := <-v; ok {
				_ = err
				out <- struct{}{}
			}
		}()
		return out
	default:
		panic("unsupported channel type")
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
	case <-time.After(lifecycleWait):
		t.Fatalf("engine Stop deadlocked")
	}
}

func waitNoNbioGoroutines(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(lifecycleWait)
	markers := []string{
		"github.com/lesismal/nbio.(*poller).",
		"github.com/lesismal/nbio/timer.(*Timer).Async",
		"github.com/lesismal/nbio/timer.(*Timer).AfterFunc",
	}
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		trace := buf[:n]
		bad := false
		for _, m := range markers {
			if bytes.Contains(trace, []byte(m)) {
				bad = true
				break
			}
		}
		if !bad {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("nbio goroutines still running after Stop:\n%s", buf[:runtime.Stack(buf, true)])
}

// trackingAllocator counts Malloc/Free with generations, so a double free or
// a free of an allocation that is still referenced is caught. Freed buffers
// are poisoned and recycled: if a write callback observes its buffer after it
// was released and reused, the payload check fails.
type trackingAllocator struct {
	mu     sync.Mutex
	live   map[*[]byte]int64
	gen    map[*[]byte]int64
	next   int64
	malloc int64
	freed  int64
	reuse  [][]byte
}

func newTrackingAllocator() *trackingAllocator {
	return &trackingAllocator{
		live: map[*[]byte]int64{},
		gen:  map[*[]byte]int64{},
	}
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	a.mu.Lock()
	defer a.mu.Unlock()
	atomic.AddInt64(&a.malloc, 1)
	a.next++
	var b []byte
	if n := len(a.reuse); n > 0 && cap(a.reuse[n-1]) >= size {
		b = a.reuse[n-1][:size]
		a.reuse = a.reuse[:n-1]
	} else {
		b = make([]byte, size)
	}
	p := &b
	if g, ok := a.live[p]; ok {
		panic(fmt.Sprintf("trackingAllocator: Malloc recycled live pointer gen=%d", g))
	}
	a.live[p] = a.next
	a.gen[p] = a.next
	return p
}

func (a *trackingAllocator) Realloc(p *[]byte, size int) *[]byte {
	if cap(*p) >= size {
		*p = (*p)[:size]
		return p
	}
	q := a.Malloc(size)
	copy(*q, *p)
	a.Free(p)
	return q
}

func (a *trackingAllocator) Append(p *[]byte, more ...byte) *[]byte {
	if cap(*p)-len(*p) >= len(more) {
		*p = append(*p, more...)
		return p
	}
	q := a.Malloc(len(*p) + len(more))
	copy(*q, *p)
	copy((*q)[len(*p):], more)
	a.Free(p)
	return q
}

func (a *trackingAllocator) AppendString(p *[]byte, s string) *[]byte {
	return a.Append(p, []byte(s)...)
}

func (a *trackingAllocator) Free(p *[]byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	g, live := a.live[p]
	if !live {
		panic(fmt.Sprintf("trackingAllocator: wild/double Free, last gen=%d", a.gen[p]))
	}
	if a.gen[p] != g {
		panic("trackingAllocator: Free of a stale allocation generation")
	}
	delete(a.live, p)
	atomic.AddInt64(&a.freed, 1)
	if cap(*p) <= 256*1024 {
		for i := range *p {
			(*p)[i] = 0xEE
		}
		b := (*p)[:cap(*p)]
		a.reuse = append(a.reuse, b)
	}
}

func (a *trackingAllocator) assertBalanced(t *testing.T) {
	t.Helper()
	waitFor(t, func() bool {
		return atomic.LoadInt64(&a.malloc) == atomic.LoadInt64(&a.freed)
	}, "allocator Malloc/Free counts to balance")
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.live) != 0 {
		t.Fatalf("allocator has %d live buffers", len(a.live))
	}
}

type lifecycleStats struct {
	opens    int64
	closes   int64
	mu       sync.Mutex
	closeErr []error
}

func (s *lifecycleStats) onOpen(c *Conn) {
	atomic.AddInt64(&s.opens, 1)
}

func (s *lifecycleStats) onClose(c *Conn, err error) {
	atomic.AddInt64(&s.closes, 1)
	s.mu.Lock()
	s.closeErr = append(s.closeErr, err)
	s.mu.Unlock()
}

func (s *lifecycleStats) errors() []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]error, len(s.closeErr))
	copy(out, s.closeErr)
	return out
}

func isPeerClosedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNABORTED) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset")
}

// newLifecycleEngine starts an engine with a loopback listener on an
// ephemeral port. The returned stats/allocator are wired to the engine.
func newLifecycleEngine(t *testing.T, conf Config, alloc *trackingAllocator, stats *lifecycleStats) *Engine {
	t.Helper()
	if conf.Network == "" {
		conf.Network = "tcp"
	}
	if len(conf.Addrs) == 0 {
		conf.Addrs = []string{"127.0.0.1:0"}
	}
	if conf.NPoller <= 0 {
		conf.NPoller = 2
	}
	conf.BodyAllocator = alloc
	g := NewEngine(conf)
	g.OnOpen(stats.onOpen)
	g.OnClose(stats.onClose)
	g.OnWrittenSize(func(c *Conn, b []byte, n int) {
		fill, _ := c.Session().(byte)
		for i := 0; i < n; i++ {
			if b[i] != fill {
				panic(fmt.Sprintf("write callback observed stale/reused buffer: got %#x want %#x", b[i], fill))
			}
		}
	})
	if err := g.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		testHookConnAfterOpen = nil
		testHookConnWrite = nil
		testHookConnFlush = nil
		testHookStopAfterStoppingSet = nil
	})
	return g
}

func fillSlice(n int, fill byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = fill
	}
	return b
}

// dialPeerAndRegister dials a throw-away std listener through g.AddConn and
// returns the nbio side together with the accepted peer conn.
func dialPeerAndRegister(t *testing.T, g *Engine, fill byte) (*Conn, net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("peer listen: %v", err)
	}
	peerCh := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			peerCh <- c
		}
		_ = ln.Close()
	}()
	raw, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("peer dial: %v", err)
	}
	nbc, err := g.AddConn(raw)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("AddConn: %v", err)
	}
	nbc.SetSession(fill)
	peer := <-peerCh
	return nbc, peer
}

func dialListener(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial listener: %v", err)
	}
	return c
}

// TestCloseDuringOnOpen pins: another goroutine calls Close while the user
// onOpen callback has not returned yet. onClose must be published exactly
// once and only after onOpen finished; repeated Close must be idempotent.
func TestCloseDuringOnOpen(t *testing.T) {
	alloc := newTrackingAllocator()
	stats := &lifecycleStats{}
	g := newLifecycleEngine(t, Config{NPoller: 2}, alloc, stats)

	var (
		target      *Conn
		onOpenIn    = make(chan struct{})
		releaseOpen = make(chan struct{})
		afterOpen   = make(chan struct{})
		closeCh     = make(chan error, 8)
	)
	g.OnOpen(func(c *Conn) {
		if c.Session() != nil {
			return // unexpected extra conn
		}
		c.SetSession(byte(1))
		target = c
		close(onOpenIn)
		<-releaseOpen
	})
	g.OnClose(func(c *Conn, err error) {
		stats.onClose(c, err)
		closeCh <- err
	})
	testHookConnAfterOpen = func(c *Conn) {
		if c == target {
			close(afterOpen)
		}
	}

	peer := dialListener(t, g.Addrs[0])
	defer func() { _ = peer.Close() }()
	<-onOpenIn

	// Multiple concurrent close requests while onOpen is still blocked.
	var closeWG sync.WaitGroup
	for i := 0; i < 4; i++ {
		closeWG.Add(1)
		go func() {
			defer closeWG.Done()
			_ = target.Close()
		}()
	}
	// Give the closers nowhere to hide: they must not publish onClose and the
	// conn is still in its opening phase.
	mustNotReceive(t, afterOpen, "before onOpen returns")
	mustNotReceive(t, closeCh, "before onOpen returns")

	close(releaseOpen)
	<-afterOpen

	waitFor(t, func() bool { return atomic.LoadInt64(&stats.closes) == 1 }, "single onClose")
	closeWG.Wait()

	if atomic.LoadInt64(&stats.opens) != 1 || atomic.LoadInt64(&stats.closes) != 1 {
		t.Fatalf("opens=%d closes=%d, want 1/1", stats.opens, stats.closes)
	}
	errs := stats.errors()
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("onClose errs = %v, want [nil]", errs)
	}

	// Idempotent teardown after the fact.
	if err := target.Close(); err != nil {
		t.Fatalf("redundant Close: %v", err)
	}
	stopEngine(t, g)
	waitNoNbioGoroutines(t)
}

// TestPeerCloseWithQueuedWrites pins: several write buffers are already
// cached in writeList when the peer shuts the read side down. Every queued
// buffer must be released exactly once, onClose fires once with a peer-close
// error, and Stop still exits.
func TestPeerCloseWithQueuedWrites(t *testing.T) {
	alloc := newTrackingAllocator()
	stats := &lifecycleStats{}
	g := newLifecycleEngine(t, Config{NPoller: 1}, alloc, stats)

	nbc, peer := dialPeerAndRegister(t, g, byte(0x5A))
	defer func() { _ = peer.Close() }()

	// The peer never drains anything: the kernel send buffer fills and the
	// nbio side keeps multiple > 64KiB segments in its writeList.
	chunkSize := maxWriteCacheOrFlushSize + 1
	chunkNum := 3
	flushEntered := make(chan struct{})
	flushRelease := make(chan struct{})
	testHookConnFlush = func(c *Conn) {
		if c != nbc {
			return
		}
		select {
		case <-flushEntered:
		default:
			close(flushEntered)
		}
		<-flushRelease
	}

	go func() {
		for i := 0; i < chunkNum; i++ {
			_, _ = nbc.Write(fillSlice(chunkSize, byte(0x5A)))
		}
	}()

	<-flushEntered
	// While the poller is parked inside flush, the writeList must contain at
	// least the distinct segments we queued.
	nbc.mux.Lock()
	queued := len(nbc.writeList)
	nbc.mux.Unlock()
	if queued < chunkNum {
		t.Fatalf("queued segments = %d, want >= %d", queued, chunkNum)
	}

	// Peer closes while queued data is pending.
	_ = peer.Close()
	close(flushRelease)

	waitFor(t, func() bool { return atomic.LoadInt64(&stats.closes) == 1 }, "single onClose after peer close")
	errs := stats.errors()
	if len(errs) != 1 || !isPeerClosedError(errs[0]) {
		t.Fatalf("onClose errs = %v, want one peer-close error", errs)
	}
	if n, _ := nbc.IsClosed(); !n {
		t.Fatalf("conn should be marked closed")
	}
	if _, err := nbc.Write(fillSlice(4, 0x5A)); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after close err = %v, want net.ErrClosed", err)
	}

	alloc.assertBalanced(t)
	stopEngine(t, g)
	alloc.assertBalanced(t)
	waitNoNbioGoroutines(t)
}

// TestAsyncWriteThenCloseInOnData pins: onData spawns an asynchronous write
// and immediately closes the conn on the poller. The write must observe the
// closed state (never touch a reused fd), onClose fires once.
func TestAsyncWriteThenCloseInOnData(t *testing.T) {
	alloc := newTrackingAllocator()
	stats := &lifecycleStats{}
	g := newLifecycleEngine(t, Config{NPoller: 2}, alloc, stats)

	nbc, peer := dialPeerAndRegister(t, g, byte(0x33))
	defer func() { _ = peer.Close() }()

	var (
		writeStarted = make(chan struct{})
		releaseWrite = make(chan struct{})
		writeDone    = make(chan error, 1)
	)
	testHookConnWrite = func(c *Conn) {
		if c != nbc {
			return
		}
		close(writeStarted)
		<-releaseWrite
	}

	g.OnData(func(c *Conn, data []byte) {
		if c != nbc {
			return
		}
		go func() {
			_, err := nbc.Write(fillSlice(64, 0x33))
			writeDone <- err
		}()
		<-writeStarted
		_ = nbc.Close()
		close(releaseWrite)
	})

	if _, err := peer.Write([]byte("kick")); err != nil {
		t.Fatalf("peer write: %v", err)
	}

	select {
	case err := <-writeDone:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("async write err = %v, want net.ErrClosed", err)
		}
	case <-time.After(lifecycleWait):
		t.Fatalf("async write goroutine never returned")
	}

	waitFor(t, func() bool { return atomic.LoadInt64(&stats.closes) == 1 }, "single onClose")
	alloc.assertBalanced(t)
	stopEngine(t, g)
	alloc.assertBalanced(t)
	waitNoNbioGoroutines(t)
}

// acceptBarrierListener holds a pre-accepted conn in its backlog and blocks
// the listener's second Accept until the barrier is released.
type acceptBarrierListener struct {
	net.Listener
	mu       sync.Mutex
	accepted int
	backlog  chan net.Conn
	release  chan struct{}
}

func (l *acceptBarrierListener) Accept() (net.Conn, error) {
	l.mu.Lock()
	n := l.accepted
	l.accepted++
	l.mu.Unlock()
	if n == 0 {
		select {
		case c := <-l.backlog:
			return c, nil
		default:
		}
	}
	if n == 1 {
		<-l.release
	}
	return l.Listener.Accept()
}

// TestAcceptDuringStop pins two windows: a new conn finishing AddConn while
// Stop holds its snapshot phase, and an accepted conn released exactly when
// Stop closes the listener. Neither conn may be skipped (Stop deadlock) or
// double-counted.
func TestAcceptDuringStop(t *testing.T) {
	alloc := newTrackingAllocator()
	stats := &lifecycleStats{}

	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	barrierLn := &acceptBarrierListener{
		Listener: rawLn,
		backlog:  make(chan net.Conn, 1),
		release:  make(chan struct{}),
	}

	// Pre-accept one connection into the listener's hand: it is returned by
	// the first Accept call once the listener goroutine runs.
	earlyPeer, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatalf("early dial: %v", err)
	}
	earlyConn, err := rawLn.Accept()
	if err != nil {
		t.Fatalf("early accept: %v", err)
	}
	barrierLn.backlog <- earlyConn

	var conf Config
	conf = Config{
		Network: "tcp",
		Addrs:   []string{"127.0.0.1:0"},
		NPoller: 3,
		Listen: func(network, addr string) (net.Listener, error) {
			if addr == conf.Addrs[0] {
				return barrierLn, nil
			}
			return net.Listen(network, addr)
		},
	}
	g := newLifecycleEngine(t, conf, alloc, stats)

	waitFor(t, func() bool { return atomic.LoadInt64(&stats.opens) == 1 }, "early conn onOpen")

	// Second conn: its AddConn races the Stop snapshot.
	lateRaw, err := net.Dial("tcp", rawLn.Addr().String())
	if err != nil {
		t.Fatalf("late dial: %v", err)
	}
	addDone := make(chan error, 1)
	var lateNBC *Conn
	go func() {
		c, aerr := g.AddConn(lateRaw)
		lateNBC = c
		addDone <- aerr
	}()

	stopReached := make(chan struct{})
	testHookStopAfterStoppingSet = func() {
		close(stopReached)
		// Stop is now committed; unblock the listener's blocked Accept.
		close(barrierLn.release)
	}

	// The AddConn goroutine either registered just before the snapshot (and
	// gets closed by Stop) or loses the race (and gets engineClosing). Both
	// outcomes must converge without a hang.
	stopEngine(t, g)
	_ = earlyPeer.Close()

	select {
	case aerr := <-addDone:
		if aerr != nil && !errors.Is(aerr, engineClosing) {
			t.Fatalf("racing AddConn err = %v", aerr)
		}
		if aerr == nil && lateNBC != nil {
			if n, _ := lateNBC.IsClosed(); !n {
				t.Fatalf("racing conn registered before Stop must be closed by Stop")
			}
		}
	case <-time.After(lifecycleWait):
		t.Fatalf("racing AddConn never finished")
	}

	waitFor(t, func() bool {
		if n, _ := lateNBC.IsClosed(); n || atomic.LoadInt64(&stats.closes) == atomic.LoadInt64(&stats.opens) {
			return true
		}
		return false
	}, "all registered conns closed")
	if atomic.LoadInt64(&stats.closes) != atomic.LoadInt64(&stats.opens) {
		t.Fatalf("opens=%d closes=%d", stats.opens, stats.closes)
	}
	alloc.assertBalanced(t)
	waitNoNbioGoroutines(t)
}

// TestCloseAgainInsideOnClose pins: the user onClose callback calls Close
// (both once more and concurrently). No panic, no second onClose, Stop exits.
func TestCloseAgainInsideOnClose(t *testing.T) {
	alloc := newTrackingAllocator()
	stats := &lifecycleStats{}
	g := newLifecycleEngine(t, Config{NPoller: 2}, alloc, stats)

	var (
		target    *Conn
		onCloseIn = make(chan struct{})
		allowOut  = make(chan struct{})
	)
	g.OnOpen(func(c *Conn) {
		c.SetSession(byte(0x77))
		target = c
	})
	g.OnClose(func(c *Conn, err error) {
		if c != target {
			return
		}
		stats.onClose(c, err)
		close(onCloseIn)
		<-allowOut
	})

	peer := dialListener(t, g.Addrs[0])
	defer func() { _ = peer.Close() }()
	waitFor(t, func() bool { return target != nil }, "onOpen")

	_ = target.Close()
	<-onCloseIn

	// Close again from the onClose goroutine itself and from others while
	// the onClose callback is still running.
	_ = target.Close()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = target.Close()
		}()
	}
	close(allowOut)
	wg.Wait()

	if atomic.LoadInt64(&stats.closes) != 1 {
		t.Fatalf("closes=%d, want 1", stats.closes)
	}
	alloc.assertBalanced(t)
	stopEngine(t, g)
	waitNoNbioGoroutines(t)
}

// opKind enumerates the fixed-seed operations used by TestSeededLifecycleRace.
type opKind int

const (
	opWrite opKind = iota
	opPeerWrite
	opPeerClose
	opConnClose
	opPausePeerRead
	opResumePeerRead
)

type seededOp struct {
	conn int
	kind opKind
	size int
}

// TestSeededLifecycleRace replays a fixed-seed operation sequence across a
// few conns: queued writes, peer writes/half-closes, conn closes and Stop
// interleave deterministically. It is aimed at mutations such as "release
// buffer before the write callback", "publish close events twice", "miss a
// poller wakeup" and "Stop deadlocks".
func TestSeededLifecycleRace(t *testing.T) {
	seeds := []int64{1, 7, 42}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runSeededLifecycleRace(t, seed)
		})
	}
}

type seededSeat struct {
	nbc       *Conn
	peer      net.Conn
	pauseRead int32
	resume    chan struct{}
	resumed   int32
}

func (s *seededSeat) wake() {
	if atomic.CompareAndSwapInt32(&s.resumed, 0, 1) {
		close(s.resume)
	}
}

func runSeededLifecycleRace(t *testing.T, seed int64) {
	alloc := newTrackingAllocator()
	stats := &lifecycleStats{}
	g := newLifecycleEngine(t, Config{NPoller: 4}, alloc, stats)

	const connNum = 4
	seats := make([]*seededSeat, connNum)
	var fills [connNum]byte
	for i := range seats {
		fill := byte(0x10 + i*0x31)
		fills[i] = fill
		nbc, peer := dialPeerAndRegister(t, g, fill)
		seats[i] = &seededSeat{nbc: nbc, peer: peer, resume: make(chan struct{})}
	}
	defer func() {
		for _, s := range seats {
			_ = s.peer.Close()
			s.wake()
		}
	}()

	// Fixed-seed operation plan.
	rng := rand.New(rand.NewSource(seed))
	ops := make([]seededOp, 0, 80)
	closedPeer := make([]bool, connNum)
	for len(ops) < 64 {
		idx := rng.Intn(connNum)
		k := opKind(rng.Intn(6))
		if k == opPeerClose && closedPeer[idx] {
			continue
		}
		op := seededOp{conn: idx, kind: k}
		if k == opWrite {
			if rng.Intn(4) == 0 {
				op.size = maxWriteCacheOrFlushSize + 1 + rng.Intn(4096)
			} else {
				op.size = 1 + rng.Intn(8192)
			}
		} else if k == opPeerWrite {
			op.size = 1 + rng.Intn(256)
		}
		ops = append(ops, op)
	}

	// Peer drainer verifies every byte until the conn breaks. It parks on
	// the resume gate while the plan pauses draining, forcing the nbio side
	// to accumulate queued write segments.
	for i, s := range seats {
		fill := fills[i]
		go func(s *seededSeat, fill byte) {
			buf := make([]byte, 64*1024)
			for {
				if atomic.LoadInt32(&s.pauseRead) == 1 {
					select {
					case <-s.resume:
					case <-time.After(lifecycleWait):
						return
					}
					continue
				}
				n, err := s.peer.Read(buf)
				if n > 0 {
					for j := 0; j < n; j++ {
						if buf[j] != fill {
							panic(fmt.Sprintf("peer got corrupted/reused data: %#x != %#x", buf[j], fill))
						}
					}
				}
				if err != nil {
					return
				}
			}
		}(s, fill)
	}

	// Step barrier: each op is issued and committed before the next one.
	paused := make([]bool, connNum)
	for _, op := range ops {
		s := seats[op.conn]
		switch op.kind {
		case opWrite:
			if !closedPeer[op.conn] {
				_, _ = s.nbc.Write(fillSlice(op.size, fills[op.conn]))
			}
		case opPeerWrite:
			if !closedPeer[op.conn] {
				if _, err := s.peer.Write(fillSlice(op.size, fills[op.conn])); err != nil {
					closedPeer[op.conn] = true
				}
			}
		case opPausePeerRead:
			if !paused[op.conn] && !closedPeer[op.conn] {
				paused[op.conn] = true
				atomic.StoreInt32(&s.pauseRead, 1)
			}
		case opResumePeerRead:
			if paused[op.conn] {
				paused[op.conn] = false
				atomic.StoreInt32(&s.pauseRead, 0)
				s.wake()
			}
		case opPeerClose:
			closedPeer[op.conn] = true
			s.wake()
			_ = s.peer.Close()
		case opConnClose:
			_ = s.nbc.Close()
		}
	}
	for _, s := range seats {
		s.wake()
	}

	stopEngine(t, g)

	if got := atomic.LoadInt64(&stats.opens); got != connNum {
		t.Fatalf("opens=%d, want %d", got, connNum)
	}
	if got := atomic.LoadInt64(&stats.closes); got != connNum {
		t.Fatalf("closes=%d, want %d", got, connNum)
	}
	if got := len(stats.errors()); got != connNum {
		t.Fatalf("onClose events=%d, want %d", got, connNum)
	}
	for i := range seats {
		if n, _ := seats[i].nbc.IsClosed(); !n {
			t.Fatalf("conn %d not closed after Stop", i)
		}
	}

	alloc.assertBalanced(t)
	waitNoNbioGoroutines(t)
}
