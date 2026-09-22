// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/nbio/mempool"
)

// countingAllocator wraps a mempool.Allocator and counts Malloc/Free calls so
// that tests can assert every queued write buffer is returned exactly once.
type countingAllocator struct {
	inner  mempool.Allocator
	malloc int64
	free   int64
}

func (a *countingAllocator) Malloc(size int) *[]byte {
	p := a.inner.Malloc(size)
	atomic.AddInt64(&a.malloc, 1)
	return p
}

func (a *countingAllocator) Free(p *[]byte) {
	atomic.AddInt64(&a.free, 1)
	a.inner.Free(p)
}

func (a *countingAllocator) Realloc(p *[]byte, size int) *[]byte {
	return a.inner.Realloc(p, size)
}

func (a *countingAllocator) Append(p *[]byte, more ...byte) *[]byte {
	return a.inner.Append(p, more...)
}

func (a *countingAllocator) AppendString(p *[]byte, more string) *[]byte {
	return a.inner.AppendString(p, more)
}

// connCounters records how many times the lifecycle callbacks fire for a
// connection and the error observed on close.
type connCounters struct {
	open      int64
	data      int64
	close     int64
	closeErr  error
	closeOnce sync.Once
	errMu     sync.Mutex
}

func newConnCounters() *connCounters {
	return &connCounters{}
}

func (cc *connCounters) onOpen(_ *Conn) {
	atomic.AddInt64(&cc.open, 1)
}

func (cc *connCounters) onData(_ *Conn, _ []byte) {
	atomic.AddInt64(&cc.data, 1)
}

func (cc *connCounters) onClose(_ *Conn, err error) {
	cc.errMu.Lock()
	cc.closeErr = err
	cc.errMu.Unlock()
	atomic.AddInt64(&cc.close, 1)
}

func (cc *connCounters) opens() int64 { return atomic.LoadInt64(&cc.open) }
func (cc *connCounters) datas() int64 { return atomic.LoadInt64(&cc.data) }
func (cc *connCounters) closes() int64 {
	return atomic.LoadInt64(&cc.close)
}

func (cc *connCounters) err() error {
	cc.errMu.Lock()
	defer cc.errMu.Unlock()
	return cc.closeErr
}

// lifecycleHooks wires a fresh engine with counting callbacks and a counting
// write-buffer allocator.
type lifecycleHooks struct {
	g     *Engine
	cc    *connCounters
	alloc *countingAllocator
}

func newLifecycleEngine(t *testing.T, npoller int,
	serverOnOpen func(*Conn),
	serverOnData func(*Conn, []byte),
	serverOnClose func(*Conn, error),
	alloc mempool.Allocator,
) *Engine {
	conf := Config{
		Network: "tcp",
		Addrs:   []string{"127.0.0.1:0"},
	}
	if npoller > 0 {
		conf.NPoller = npoller
	}
	if alloc != nil {
		conf.BodyAllocator = alloc
	}
	g := NewEngine(conf)
	if serverOnOpen != nil {
		g.OnOpen(serverOnOpen)
	}
	if serverOnData != nil {
		g.OnData(serverOnData)
	}
	if serverOnClose != nil {
		g.OnClose(serverOnClose)
	}
	if err := g.Start(); err != nil {
		t.Fatalf("engine start: %v", err)
	}
	return g
}

func dialLoopback(t *testing.T, g *Engine) (net.Conn, *Conn) {
	t.Helper()
	raw, err := net.Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c, err := g.AddConn(raw)
	if err != nil {
		_ = raw.Close()
		t.Fatalf("addconn: %v", err)
	}
	return raw, c
}

// stopEngineWithWatchdog calls Stop in a goroutine and fails the test if it
// does not return before the watchdog deadline, proving every goroutine the
// Engine owns is able to exit.
func stopEngineWithWatchdog(t *testing.T, g *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("Engine.Stop deadlocked")
	}
}

// waitFor polls cond until it returns true or the deadline passes. It never
// sleeps inside the product; the short period only polls test channels.
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		runtime.Gosched()
	}
}
