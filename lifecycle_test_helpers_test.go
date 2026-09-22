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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/nbio/mempool"
)

// lifecycleTestTimeout is the watchdog for every blocking assertion in
// the lifecycle tests. Failures surface as t.Fatalf instead of hangs;
// normal completions are event-driven and never wait for this duration.
const lifecycleTestTimeout = 5 * time.Second

// waitSignal waits for ch with a watchdog.
func waitSignal(t *testing.T, ch <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timeout waiting for %s", msg)
	}
}

// stopEngine runs g.Stop under a watchdog so that a deadlocked shutdown
// fails the test instead of hanging the whole run.
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

// resetLifecycleHooks clears all global test hooks.
func resetLifecycleHooks() {
	hookAfterAccept = nil
	hookStopAfterConns = nil
	hookPollerStop = nil
}

// newListeningEngine creates an Engine listening on a random loopback
// tcp port (no external network). configure runs before Start so that
// callbacks are registered before any poller goroutine can observe a
// connection (keeps the race detector's happens-before graph intact).
// The returned stop func is idempotent so tests may stop manually and
// still rely on cleanup.
func newListeningEngine(t *testing.T, conf Config, configure func(g *Engine)) (*Engine, func()) {
	t.Helper()
	if conf.Network == "" {
		conf.Network = "tcp"
	}
	conf.Addrs = []string{"127.0.0.1:0"}
	g := NewEngine(conf)
	if configure != nil {
		configure(g)
	}
	if err := g.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() { g.Stop() })
	}
	t.Cleanup(func() {
		done := make(chan struct{})
		go func() {
			stop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(lifecycleTestTimeout):
			t.Fatalf("Engine.Stop deadlocked in cleanup")
		}
	})
	return g, stop
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	return conn
}

// connCounters tracks once-only callback invocations per *Conn.
type connCounters struct {
	mu     sync.Mutex
	opens  map[*Conn]int
	closes map[*Conn]int
	order  []string // "open:<fd>" / "close:<fd>" on this counter set
}

func newConnCounters() *connCounters {
	return &connCounters{
		opens:  map[*Conn]int{},
		closes: map[*Conn]int{},
	}
}

func (cc *connCounters) markOpen(c *Conn) {
	cc.mu.Lock()
	cc.opens[c]++
	cc.order = append(cc.order, fmt.Sprintf("open:%d", c.fdOrHash()))
	cc.mu.Unlock()
}

func (cc *connCounters) markClose(c *Conn) {
	cc.mu.Lock()
	cc.closes[c]++
	cc.order = append(cc.order, fmt.Sprintf("close:%d", c.fdOrHash()))
	cc.mu.Unlock()
}

func (cc *connCounters) totalCloses() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	n := 0
	for _, v := range cc.closes {
		n += v
	}
	return n
}

func (cc *connCounters) totalOpens() int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	n := 0
	for _, v := range cc.opens {
		n += v
	}
	return n
}

func (cc *connCounters) closeCount(c *Conn) int {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.closes[c]
}

// assertOnceEach verifies OnOpen/OnClose ran exactly once for each conn
// that was ever opened, and that for every single conn its open entry
// precedes its close entry (per-conn order only, no global ordering).
func (cc *connCounters) assertOnceEach(t *testing.T) {
	t.Helper()
	cc.mu.Lock()
	defer cc.mu.Unlock()
	openPos := map[string]int{}
	closeCount := map[string]int{}
	openCount := map[string]int{}
	for i, ev := range cc.order {
		if strings.HasPrefix(ev, "open:") {
			openCount[ev[5:]]++
			openPos[ev[5:]] = i
		} else {
			id := ev[6:]
			closeCount[id]++
			if openCount[id] == 0 || i < openPos[id] {
				t.Errorf("callback order violated for conn %s: close at %d, open events seen %d", id, i, openCount[id])
			}
		}
	}
	for c, n := range cc.opens {
		id := fmt.Sprintf("%d", c.fdOrHash())
		if n != 1 {
			t.Errorf("conn %s OnOpen called %d times, want 1", id, n)
		}
		if closeCount[id] != 1 {
			t.Errorf("conn %s OnClose called %d times, want 1", id, closeCount[id])
		}
	}
	for id, n := range closeCount {
		if n != 1 {
			t.Errorf("conn %s OnClose called %d times, want 1", id, n)
		}
	}
}

// fdOrHash returns the unix fd or the std hash, both uniquely identify
// the Conn within a test.
//go:norace
func (c *Conn) fdOrHash() int {
	return connID(c)
}

// trackingAllocator wraps a mempool.Allocator and records every live
// buffer. It fails the test on double free and poisons released memory
// so that a write happening after the buffer was released corrupts the
// echoed payload deterministically.
type trackingAllocator struct {
	base mempool.Allocator

	mu     sync.Mutex
	live   map[uintptr]int // base ptr -> size
	freed  map[uintptr]int // base ptr -> size (poisoned)
	active int64
	totalM int64
	totalF int64
	t      *testing.T
}

func newTrackingAllocator(t *testing.T) *trackingAllocator {
	return &trackingAllocator{
		base:  mempool.New(64*1024, 1024*1024),
		live:  map[uintptr]int{},
		freed: map[uintptr]int{},
		t:     t,
	}
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	p := a.base.Malloc(size)
	ptr := basePtr(p)
	a.mu.Lock()
	if _, ok := a.live[ptr]; ok {
		a.t.Errorf("trackingAllocator: buffer %x already live on Malloc", ptr)
	}
	a.live[ptr] = cap(*p)
	delete(a.freed, ptr)
	n := atomic.AddInt64(&a.totalM, 1)
	_ = n
	a.mu.Unlock()
	atomic.AddInt64(&a.active, 1)
	return p
}

func (a *trackingAllocator) Realloc(buf *[]byte, size int) *[]byte {
	oldPtr := basePtr(buf)
	newBuf := a.base.Realloc(buf, size)
	newPtr := basePtr(newBuf)
	if newPtr != oldPtr {
		a.mu.Lock()
		if _, ok := a.live[oldPtr]; !ok {
			a.t.Errorf("trackingAllocator: Realloc unknown buffer %x", oldPtr)
		}
		delete(a.live, oldPtr)
		a.live[newPtr] = cap(*newBuf)
		a.mu.Unlock()
	}
	return newBuf
}

func (a *trackingAllocator) Append(buf *[]byte, more ...byte) *[]byte {
	return a.base.Append(buf, more...)
}

func (a *trackingAllocator) AppendString(buf *[]byte, more string) *[]byte {
	return a.base.AppendString(buf, more)
}

func (a *trackingAllocator) Free(buf *[]byte) {
	if buf == nil {
		return
	}
	ptr := basePtr(buf)
	a.mu.Lock()
	size, ok := a.live[ptr]
	if !ok {
		a.mu.Unlock()
		a.t.Errorf("trackingAllocator: double or stray Free of buffer %x (size %d, cap %d)", ptr, size, cap(*buf))
		return
	}
	delete(a.live, ptr)
	a.freed[ptr] = size
	// poison: any use-after-release writes 0xdd into peer-visible data.
	for i := range *buf {
		(*buf)[i] = 0xdd
	}
	atomic.AddInt64(&a.totalF, 1)
	a.mu.Unlock()
	atomic.AddInt64(&a.active, -1)
	a.base.Free(buf)
}

// owns reports whether b is backed by a currently live malloc buffer,
// i.e. base <= b < base+size.
func (a *trackingAllocator) owns(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	p := uintptr(unsafe.Pointer(&b[0]))
	a.mu.Lock()
	defer a.mu.Unlock()
	for base, size := range a.live {
		if p >= base && p < base+uintptr(size) {
			return true
		}
	}
	return false
}

func (a *trackingAllocator) assertBalanced(t *testing.T) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if n := atomic.LoadInt64(&a.active); n != 0 {
		t.Errorf("trackingAllocator: %d buffers leaked, %d live entries", n, len(a.live))
	}
	if a.totalM != a.totalF {
		t.Errorf("trackingAllocator: Malloc %d != Free %d", a.totalM, a.totalF)
	}
	if len(a.live) != 0 {
		t.Errorf("trackingAllocator: %d buffers never released", len(a.live))
	}
}

//go:norace
func basePtr(p *[]byte) uintptr {
	if p == nil || len(*p) == 0 && cap(*p) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&(*p)[:1][0]))
}

// isPeerClosedError reports whether err represents the peer closing
// the connection (EOF/reset), as opposed to a local close.
func isPeerClosedError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	msg := err.Error()
	if strings.Contains(msg, "reset") || strings.Contains(msg, "broken pipe") {
		return true
	}
	return isPeerClosedSyscallErr(err)
}

// isLocalClosed reports a locally-initiated close error category.
func isLocalClosed(err error) bool {
	return errors.Is(err, net.ErrClosed)
}

// goroutineSnapshot captures nbio-package goroutine stack signatures.
type goroutineSnapshot map[string]int

func takeGoroutineSnapshot() goroutineSnapshot {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	snap := goroutineSnapshot{}
	for _, g := range strings.Split(string(buf[:n]), "\n\n") {
		if !strings.Contains(g, "/nbio") {
			continue
		}
		lines := strings.Split(g, "\n")
		if len(lines) < 2 {
			continue
		}
		// first stack frame signature
		sig := strings.TrimSpace(lines[1])
		snap[sig]++
	}
	return snap
}

// assertNoNbioGoroutineLeak compares a baseline snapshot with a post
// test one; poller/timer goroutines that belonged to a stopped engine
// must all have exited. Retries briefly to let async workers drain.
func assertNoNbioGoroutineLeak(t *testing.T, base goroutineSnapshot) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var after goroutineSnapshot
	for time.Now().Before(deadline) {
		after = takeGoroutineSnapshot()
		leaked := false
		for sig, n := range after {
			if n > base[sig] {
				leaked = true
				break
			}
		}
		if !leaked {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	for sig, n := range after {
		if n > base[sig] {
			t.Errorf("leaked nbio goroutine: %s: %d > %d", sig, n, base[sig])
		}
	}
}

// installLeakCheck captures a baseline in t.Cleanup order before the
// test creates engines.
func installLeakCheck(t *testing.T) {
	t.Helper()
	base := takeGoroutineSnapshot()
	t.Cleanup(func() {
		assertNoNbioGoroutineLeak(t, base)
	})
}

// sortConnsByID orders conns by their platform conn id for deterministic
// seed assignment (no global event ordering is assumed).
//go:norace
func sortConnsByID(cs []*Conn) {
	for i := 1; i < len(cs); i++ {
		for j := i; j > 0 && connID(cs[j-1]) > connID(cs[j]); j-- {
			cs[j-1], cs[j] = cs[j], cs[j-1]
		}
	}
}
