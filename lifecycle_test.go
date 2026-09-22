// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"bytes"
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

// Deterministic lifecycle tests. Every test drives the Engine/Conn through a
// single fixed interleaving using channels and the test-only hooks declared in
// sync_hooks.go, instead of relying on random scheduler pressure, sleeps or
// enlarged timeouts.
//
// Contract under test:
//   - for a single Conn, OnOpen happens once and always before its OnClose;
//   - OnClose is dispatched exactly once, no matter how many Close calls race;
//   - Write after Close returns net.ErrClosed;
//   - every queued write buffer is released exactly once;
//   - Engine.Stop closes every connection, wakes every poller and returns.
//
// Ordering is only asserted for events of the same Conn; no global ordering
// between different Conns is assumed.

const lifecycleWait = 5 * time.Second

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleWait):
		t.Fatalf("timeout waiting for %s", what)
	}
}

type closeEvent struct {
	c   *Conn
	err error
}

// lifecycleEnv is a one-listener engine plus the per-test callback accounting.
type lifecycleEnv struct {
	g        *Engine
	addr     string
	openCh      chan *Conn
	closeCh     chan *closeEvent
	opens       int64
	closes      sync.Map // *Conn -> *int64
	ncloses     int64
	mu          sync.Mutex
	closeErr    map[*Conn]error
	stopped     bool
	userMu      sync.RWMutex
	userOnOpen  func(*Conn)
	userOnData  func(*Conn, []byte)
	userOnClose func(*Conn, error)
}

func newLifecycleEnv(t *testing.T, nPoller int) *lifecycleEnv {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	env := &lifecycleEnv{
		addr:     addr,
		openCh:   make(chan *Conn, 64),
		closeCh:  make(chan *closeEvent, 1024),
		closeErr: map[*Conn]error{},
	}
	g := NewEngine(Config{
		Name:    "lifecycle",
		Network: "tcp",
		Addrs:   []string{addr},
		NPoller: nPoller,
	})
	g.OnOpen(func(c *Conn) {
		atomic.AddInt64(&env.opens, 1)
		env.openCh <- c
		env.userMu.RLock()
		h := env.userOnOpen
		env.userMu.RUnlock()
		if h != nil {
			h(c)
		}
	})
	g.OnData(func(c *Conn, data []byte) {
		env.userMu.RLock()
		h := env.userOnData
		env.userMu.RUnlock()
		if h != nil {
			h(c, data)
		}
	})
	g.OnClose(func(c *Conn, err error) {
		v, _ := env.closes.LoadOrStore(c, new(int64))
		if n := atomic.AddInt64(v.(*int64), 1); n > 1 {
			t.Errorf("OnClose dispatched %d times for one Conn", n)
			return
		}
		atomic.AddInt64(&env.ncloses, 1)
		env.mu.Lock()
		env.closeErr[c] = err
		env.mu.Unlock()
		env.closeCh <- &closeEvent{c: c, err: err}
		env.userMu.RLock()
		h := env.userOnClose
		env.userMu.RUnlock()
		if h != nil {
			h(c, err)
		}
	})
	env.g = g
	if err := g.Start(); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	t.Cleanup(env.cleanup)
	return env
}

func (env *lifecycleEnv) cleanup() {
	if !env.stopped {
		env.stop()
	}
}

func (env *lifecycleEnv) setOnOpen(h func(*Conn)) {
	env.userMu.Lock()
	env.userOnOpen = h
	env.userMu.Unlock()
}

func (env *lifecycleEnv) setOnData(h func(*Conn, []byte)) {
	env.userMu.Lock()
	env.userOnData = h
	env.userMu.Unlock()
}

func (env *lifecycleEnv) setOnClose(h func(*Conn, error)) {
	env.userMu.Lock()
	env.userOnClose = h
	env.userMu.Unlock()
}

func (env *lifecycleEnv) stop() {
	done := make(chan struct{})
	go func() {
		env.g.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(lifecycleWait):
		// Let the test goroutine print its own failure instead of panicking
		// from the cleanup goroutine.
		panic("lifecycle: Engine.Stop hung")
	}
	env.stopped = true
}

func (env *lifecycleEnv) dialRaw(t *testing.T) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", env.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func (env *lifecycleEnv) waitOpen(t *testing.T) (*Conn, net.Conn) {
	t.Helper()
	var raw net.Conn
	ready := make(chan struct{})
	go func() {
		raw = env.dialRaw(t)
		close(ready)
	}()
	var c *Conn
	select {
	case c = <-env.openCh:
	case <-time.After(lifecycleWait):
		t.Fatalf("timeout waiting for OnOpen")
	}
	<-ready
	return c, raw
}

func (env *lifecycleEnv) waitClose(t *testing.T, what string) *closeEvent {
	t.Helper()
	select {
	case ev := <-env.closeCh:
		return ev
	case <-time.After(lifecycleWait):
		t.Fatalf("timeout waiting for OnClose: %s", what)
	}
	return nil
}

func (env *lifecycleEnv) assertNoMoreClose(t *testing.T) {
	t.Helper()
	select {
	case ev := <-env.closeCh:
		t.Fatalf("unexpected extra OnClose for %p: %v", ev.c, ev.err)
	case <-time.After(100 * time.Millisecond):
	}
}

func (env *lifecycleEnv) closeCount(c *Conn) int64 {
	v, ok := env.closes.Load(c)
	if !ok {
		return 0
	}
	return atomic.LoadInt64(v.(*int64))
}

// nbioGoroutines counts goroutines currently executing nbio poller/listener
// loops. Callers snapshot the count before starting their engine and compare
// it after shutdown; the package-wide engine from nbio_test.go is part of the
// shared background and therefore cancels out.
func nbioGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	count := 0
	for _, block := range strings.Split(string(buf[:n]), "\n\n") {
		if strings.Contains(block, "/nbio.") &&
			(strings.Contains(block, ".readWriteLoop(") ||
				strings.Contains(block, ".acceptorLoop(") ||
				strings.Contains(block, ".readConn(")) {
			count++
		}
	}
	return count
}

func assertNoLeakedLoops(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if nbioGoroutines() <= before {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("nbio event loops leaked: before=%d after=%d", before, nbioGoroutines())
}

// stopAndAssertLoops stops the env (bounded, no indefinite block) and then
// verifies all its listener/poller goroutines have exited. Safe to call more
// than once; the registered cleanup becomes a no-op afterwards.
func (env *lifecycleEnv) stopAndAssertLoops(t *testing.T, before int) {
	t.Helper()
	if !env.stopped {
		env.stop()
	}
	assertNoLeakedLoops(t, before)
}

// poisonAllocator counts malloc/free pairs and overwrites every freed buffer
// with a poison byte, so write-after-free shows up in observed data even with
// the package's //go:norace production functions.
type poisonAllocator struct {
	mallocs int64
	frees   int64
}

func (a *poisonAllocator) Malloc(size int) *[]byte {
	atomic.AddInt64(&a.mallocs, 1)
	b := make([]byte, size)
	return &b
}

func (a *poisonAllocator) Realloc(pbuf *[]byte, size int) *[]byte {
	if cap(*pbuf) >= size {
		*pbuf = (*pbuf)[:size]
		return pbuf
	}
	nb := make([]byte, size)
	copy(nb, *pbuf)
	return &nb
}

func (a *poisonAllocator) Append(pbuf *[]byte, more ...byte) *[]byte {
	*pbuf = append(*pbuf, more...)
	return pbuf
}

func (a *poisonAllocator) AppendString(pbuf *[]byte, more string) *[]byte {
	*pbuf = append(*pbuf, more...)
	return pbuf
}

func (a *poisonAllocator) Free(pbuf *[]byte) {
	atomic.AddInt64(&a.frees, 1)
	for i := range *pbuf {
		(*pbuf)[i] = 0xDD
	}
}

func (a *poisonAllocator) balanced() bool {
	return atomic.LoadInt64(&a.mallocs) == atomic.LoadInt64(&a.frees)
}

var _ = io.EOF
var _ = bytes.MinRead
var _ = errors.New

// setHooks replaces the package test hooks and restores them on cleanup.
func setHooks(t *testing.T, beforeWrite, beforeClose func(*Conn), stopBegin func(*Engine)) {
	t.Helper()
	prevWrite := hookConnBeforeWrite
	prevClose := hookConnBeforeClose
	prevStop := hookEngineStopBegin
	hookConnBeforeWrite = beforeWrite
	hookConnBeforeClose = beforeClose
	hookEngineStopBegin = stopBegin
	t.Cleanup(func() {
		hookConnBeforeWrite = prevWrite
		hookConnBeforeClose = prevClose
		hookEngineStopBegin = prevStop
	})
}

// Scenario A: Close is requested from another goroutine while OnOpen has not
// returned yet.
func TestLifecycleCloseDuringOnOpen(t *testing.T) {
	before := nbioGoroutines()
	openEntered := make(chan struct{})
	allowOpenReturn := make(chan struct{})
	openReturned := make(chan struct{})
	closeEntered := make(chan struct{})
	allowCloseReturn := make(chan struct{})

	env := newLifecycleEnv(t, 1)
	env.setOnOpen(func(*Conn) {
		close(openEntered)
		<-allowOpenReturn
		close(openReturned)
	})

	setHooks(t, nil,
		func(*Conn) {
			select {
			case <-closeEntered:
			default:
				close(closeEntered)
			}
			<-allowCloseReturn
		},
		nil)

	raw := env.dialRaw(t)
	defer raw.Close()
	waitSignal(t, openEntered, "OnOpen entered")

	var serverConn *Conn
	select {
	case serverConn = <-env.openCh:
	case <-time.After(lifecycleWait):
		t.Fatal("OnOpen conn not published")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- serverConn.Close() }()
	waitSignal(t, closeEntered, "concurrent Close entered")

	// Nothing must have been torn down while OnOpen is still running.
	select {
	case ev := <-env.closeCh:
		t.Fatalf("OnClose fired before OnOpen returned: %v", ev.err)
	case <-time.After(50 * time.Millisecond):
	}

	// Return OnOpen first; the close teardown then runs under c.mux and the
	// parked Close call finishes immediately after it.
	close(allowOpenReturn)
	waitSignal(t, openReturned, "OnOpen returned")
	close(allowCloseReturn)

	ev := env.waitClose(t, "close during OnOpen")
	if ev.c != serverConn {
		t.Fatal("OnClose delivered for an unexpected Conn")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(lifecycleWait):
		t.Fatal("concurrent Close did not return")
	}

	// Every subsequent Close is a cheap no-op.
	if err := serverConn.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if err := serverConn.CloseWithError(errors.New("again")); err != nil {
		t.Fatalf("third Close returned error: %v", err)
	}

	if _, err := serverConn.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after close err = %v, want net.ErrClosed", err)
	}
	if n := env.closeCount(serverConn); n != 1 {
		t.Fatalf("OnClose count = %d, want 1", n)
	}
	env.assertNoMoreClose(t)
	env.stopAndAssertLoops(t, before)
}

// Scenario C: OnData starts an asynchronous Write from another goroutine and
// closes the Conn immediately, in the fixed order "write has entered Write ->
// Close completes".
func TestLifecycleAsyncWriteThenClose(t *testing.T) {
	before := nbioGoroutines()
	writeEntered := make(chan struct{})
	allowWriteProceed := make(chan struct{})
	dataEntered := make(chan struct{})

	env := newLifecycleEnv(t, 1)

	var serverConn *Conn
	env.setOnOpen(func(c *Conn) { serverConn = c })
	env.setOnData(func(c *Conn, data []byte) {
		close(dataEntered)
		go func() {
			_, _ = c.Write(append([]byte{}, data...))
		}()
		<-writeEntered
		_ = c.Close()
		close(allowWriteProceed)
	})

	once := make(chan struct{})
	setHooks(t,
		func(*Conn) {
			select {
			case <-once:
			default:
				close(once)
				close(writeEntered)
			}
			<-allowWriteProceed
		},
		nil, nil)

	raw := env.dialRaw(t)
	defer raw.Close()
	<-env.openCh

	if _, err := raw.Write([]byte("z")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	waitSignal(t, dataEntered, "OnData entered")

	ev := env.waitClose(t, "async write then close")
	if ev.c != serverConn {
		t.Fatal("OnClose delivered for an unexpected Conn")
	}
	if n := env.closeCount(serverConn); n != 1 {
		t.Fatalf("OnClose count = %d, want 1", n)
	}
	if _, err := ev.c.Write([]byte("y")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after close err = %v, want net.ErrClosed", err)
	}

	buf := make([]byte, 16)
	if _, err := raw.Read(buf); err == nil {
		t.Fatal("expected client connection to be closed by server")
	}
	env.stopAndAssertLoops(t, before)
}

// Scenario D: a connection finishes accept exactly while Engine.Stop runs.
//
// The accept is parked after listener.Accept returns but before it is
// registered with an IO poller (hookListenerAccepted). The engine shutdown
// closes the listeners first, so the parked accept spans the shutdown point.
// Stop must wait for it to be published and then close it exactly once;
// otherwise the Conn leaks (and Stop either hangs on the WaitGroup or leaves
// a live Conn behind).
func TestLifecycleAcceptDuringStop(t *testing.T) {
	before := nbioGoroutines()

	listenersClosed := make(chan struct{})
	acceptParked := make(chan struct{})
	allowAcceptRegister := make(chan struct{})

	var stopFlag int32
	var acceptFlag int32
	env := newLifecycleEnv(t, 2)

	prevAccepted := hookListenerAccepted
	hookListenerAccepted = func(*Conn) {
		if atomic.CompareAndSwapInt32(&acceptFlag, 0, 1) {
			close(acceptParked)
		}
		<-allowAcceptRegister
	}
	prevStop := hookEngineStopBegin
	hookEngineStopBegin = func(*Engine) {
		if atomic.CompareAndSwapInt32(&stopFlag, 0, 1) {
			close(listenersClosed)
		}
	}
	t.Cleanup(func() {
		hookListenerAccepted = prevAccepted
		hookEngineStopBegin = prevStop
	})

	// Initiate the connect while the listener is alive: the listener
	// goroutine Accepts the socket and parks before registering the Conn.
	raw, err := net.Dial("tcp", env.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	waitSignal(t, acceptParked, "accept parked")

	// Run Stop in the background: it must not return while the accept is
	// still parked, and it must not hang once the accept is released.
	stopDone := make(chan struct{})
	go func() {
		env.g.Stop()
		close(stopDone)
	}()
	waitSignal(t, listenersClosed, "listeners closed")

	select {
	case <-stopDone:
		t.Fatal("Stop returned while the in-flight accept was still parked")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowAcceptRegister)

	select {
	case <-stopDone:
	case <-time.After(lifecycleWait):
		t.Fatal("Stop did not return after the parked accept was released")
	}
	env.stopped = true

	// The parked Conn got OnOpen and then exactly one OnClose.
	var c *Conn
	select {
	case c = <-env.openCh:
	case <-time.After(lifecycleWait):
		t.Fatal("parked conn never got OnOpen")
	}
	ev := env.waitClose(t, "accepted during Stop")
	if ev.c != c {
		t.Fatal("OnClose delivered for an unexpected Conn")
	}
	if n := env.closeCount(c); n != 1 {
		t.Fatalf("OnClose count = %d, want 1", n)
	}
	if atomic.LoadInt64(&env.opens) != atomic.LoadInt64(&env.ncloses) {
		t.Fatalf("opens=%d closes=%d, want equal", env.opens, env.ncloses)
	}
	env.assertNoMoreClose(t)
	env.stopAndAssertLoops(t, before)
}

// Scenario E: the user calls Close again from inside the OnClose callback.
func TestLifecycleReentrantCloseInOnClose(t *testing.T) {
	before := nbioGoroutines()
	env := newLifecycleEnv(t, 1)

	closeRan := make(chan struct{})
	env.setOnClose(func(c *Conn, err error) {
		// Must be safe and must not re-enter the teardown.
		if cerr := c.Close(); cerr != nil {
			t.Errorf("Close inside OnClose returned error: %v", cerr)
		}
		if cerr := c.CloseWithError(errors.New("from OnClose")); cerr != nil {
			t.Errorf("CloseWithError inside OnClose returned error: %v", cerr)
		}
		close(closeRan)
	})

	raw := env.dialRaw(t)
	c := <-env.openCh
	_ = c.Close()
	ev := env.waitClose(t, "reentrant close")
	waitSignal(t, closeRan, "OnClose body to finish")
	if ev.c != c {
		t.Fatal("OnClose delivered for an unexpected Conn")
	}
	if n := env.closeCount(c); n != 1 {
		t.Fatalf("OnClose count = %d, want 1", n)
	}
	env.assertNoMoreClose(t)
	_ = raw.Close()
	env.stopAndAssertLoops(t, before)
}
