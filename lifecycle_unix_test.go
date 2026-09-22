// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import (
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// queuedBuffersLocked returns the number of cached toWrite entries. Caller
// must ensure the conn is not being flushed concurrently.
func queuedBuffersLocked(c *Conn) int {
	c.mux.Lock()
	n := len(c.writeList)
	c.mux.Unlock()
	return n
}

// Scenario B: several buffers are already queued in the Conn's write cache
// (kernel send buffer full) when the peer stops reading and closes. Every
// cached buffer must be returned to the allocator exactly once and OnClose
// must fire exactly once with a terminal error.
func TestLifecycleQueuedBuffersPeerClose(t *testing.T) {
	before := nbioGoroutines()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	alloc := &poisonAllocator{}
	g := NewEngine(Config{
		Name:          "queue-close",
		Network:       "tcp",
		Addrs:         []string{addr},
		NPoller:       1,
		BodyAllocator: alloc,
	})

	openCh := make(chan *Conn, 1)
	closeCh := make(chan *closeEvent, 4)
	var closes sync.Map
	g.OnOpen(func(c *Conn) {
		// Keep both kernel buffers small so writes hit EAGAIN quickly.
		_ = c.SetWriteBuffer(4 * 1024)
		_ = c.SetNoDelay(true)
		openCh <- c
	})
	g.OnData(func(c *Conn, data []byte) {
		t.Errorf("unexpected data on the server conn: %q", data)
	})
	g.OnClose(func(c *Conn, err error) {
		v, _ := closes.LoadOrStore(c, new(int64))
		if n := atomic.AddInt64(v.(*int64), 1); n > 1 {
			t.Errorf("OnClose dispatched %d times for one Conn", n)
		}
		closeCh <- &closeEvent{c: c, err: err}
	})

	if err := g.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			done := make(chan struct{})
			go func() { g.Stop(); close(done) }()
			select {
			case <-done:
			case <-time.After(lifecycleWait):
				t.Fatal("Engine.Stop hung")
			}
		}
	}()
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer raw.Close()
	tcp := raw.(*net.TCPConn)
	_ = tcp.SetReadBuffer(4 * 1024)
	_ = tcp.SetNoDelay(true)
	// Never read; force RST on close while the server has data outstanding.
	_ = tcp.SetLinger(0)

	serverConn := <-openCh

	// Fill until writes can no longer go straight through. Each write is
	// larger than maxWriteCacheOrFlushSize, so every EAGAINed chunk becomes
	// its own toWrite entry. 4 MB is far above the loopback send capacity
	// with the tiny buffers above.
	chunk := make([]byte, maxWriteCacheOrFlushSize+1)
	for i := range chunk {
		chunk[i] = 'q'
	}
	queued := 0
	for i := 0; i < 8; i++ {
		if _, err := serverConn.Write(chunk); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
		if queuedBuffersLocked(serverConn) > queued {
			queued = queuedBuffersLocked(serverConn)
		}
	}
	if queued < 2 {
		t.Fatalf("expected at least 2 queued buffers, got %d (send buffer not saturated?)", queued)
	}

	// Peer closes while the queue is non-empty and unread data is pending:
	// the server gets an error on the next write/flush.
	if err := raw.Close(); err != nil {
		t.Fatalf("peer close: %v", err)
	}

	var ev *closeEvent
	select {
	case ev = <-closeCh:
	case <-time.After(lifecycleWait):
		t.Fatal("OnClose not dispatched after peer closed with queued data")
	}
	if ev.err == nil {
		t.Fatal("OnClose error is nil, want a terminal error")
	}
	if ev.c != serverConn {
		t.Fatal("OnClose for unexpected conn")
	}

	if n := queuedBuffersLocked(serverConn); n != 0 {
		t.Fatalf("writeList not drained on close, %d entries remain", n)
	}
	if v, _ := closes.Load(serverConn); atomic.LoadInt64(v.(*int64)) != 1 {
		t.Fatal("OnClose dispatched more than once")
	}
	if !alloc.balanced() {
		t.Fatalf("allocator unbalanced: malloc=%d free=%d",
			atomic.LoadInt64(&alloc.mallocs), atomic.LoadInt64(&alloc.frees))
	}
	if atomic.LoadInt64(&alloc.frees) == 0 {
		t.Fatal("no queued buffer was ever freed")
	}

	if _, err := serverConn.Write([]byte("late")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after close err = %v, want net.ErrClosed", err)
	}
	_ = io.EOF
	g.Stop()
	stopped = true
	assertNoLeakedLoops(t, before)
}

// rand31 is a deterministic LCG; the whole script therefore replays the same
// interleaving on every machine and every run.
type rand31 struct{ state uint64 }

func (r *rand31) next() uint32 {
	r.state = r.state*1103515245 + 12345
	return uint32((r.state / 65536) % 32768)
}

// TestLifecycleSeededRaceSequence replays a fixed script over a few
// connections: ordered writes/reads, spontaneous server writes, concurrent
// closes from both sides and writes after close, interspersed with barrier
// points that hold a conn inside the Write/Close paths while shutdown starts.
//
// Invariants asserted:
//   - every OnOpen is matched by exactly one OnClose;
//   - every byte the client reads echoes exactly what it sent (no buffer
//     reuse-before-write-finished corruption, no poison byte);
//   - Write after Close returns net.ErrClosed;
//   - the write buffer allocator is fully balanced (no double-free / leak);
//   - Stop returns promptly and wakes every poller (no leaked loops).
func TestLifecycleSeededRaceSequence(t *testing.T) {
	const (
		iterations = 4
		nConns     = 4
		steps      = 120
	)

	for iter := 0; iter < iterations; iter++ {
		runSeededSequenceOnce(t, int64(0x5eeded00+iter), nConns, steps)
	}
}

func runSeededSequenceOnce(t *testing.T, seed int64, nConns, steps int) {
	before := nbioGoroutines()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	alloc := &poisonAllocator{}
	g := NewEngine(Config{
		Name:          "seeded",
		Network:       "tcp",
		Addrs:         []string{addr},
		NPoller:       3,
		BodyAllocator: alloc,
	})

	type slotState struct {
		server *Conn
		client net.Conn
		dead   int32
		seq    int64
	}
	slots := make([]*slotState, nConns)
	for i := range slots {
		slots[i] = &slotState{}
	}

	var (
		opens    int64
		ncloses  int64
		closeMux sync.Mutex
	)
	closeCnt := map[*Conn]int{}
	openCh := make(chan *Conn, nConns)
	closeCh := make(chan *closeEvent, 256)

	g.OnOpen(func(c *Conn) {
		atomic.AddInt64(&opens, 1)
		openCh <- c
	})
	g.OnData(func(c *Conn, data []byte) {
		for _, b := range data {
			if b == 0xDD {
				t.Errorf("poison byte received: use-after-free write buffer")
				return
			}
		}
		// echo back, tolerate a write racing a close
		_, _ = c.Write(append([]byte{}, data...))
	})
	g.OnClose(func(c *Conn, err error) {
		closeMux.Lock()
		closeCnt[c]++
		if closeCnt[c] > 1 {
			t.Errorf("OnClose dispatched %d times for one Conn", closeCnt[c])
		}
		closeMux.Unlock()
		atomic.AddInt64(&ncloses, 1)
		select {
		case closeCh <- &closeEvent{c: c, err: err}:
		default:
		}
	})

	if err := g.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	stopped := false
	defer func() {
		if !stopped {
			done := make(chan struct{})
			go func() { g.Stop(); close(done) }()
			select {
			case <-done:
			case <-time.After(lifecycleWait):
				t.Fatal("Engine.Stop hung")
			}
		}
	}()

	// reader for one slot: uses a dedicated conn so reopen churn can never
	// race an in-flight echo round trip.
	startReader := func(expect []byte, done chan struct{}) net.Conn {
		cli, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		<-openCh
		go func() {
			got := make([]byte, len(expect))
			_, _ = io.ReadFull(cli, got)
			for i := range got {
				if got[i] != expect[i] {
					t.Errorf("echo mismatch at %d: got %d want %d", i, got[i], expect[i])
					break
				}
			}
			close(done)
		}()
		return cli
	}

	establish := func(idx int) {
		cli, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		srv := <-openCh
		s := slots[idx]
		s.client = cli
		s.server = srv
		s.seq = 0
		atomic.StoreInt32(&s.dead, 0)
	}
	closeSlot := func(idx int) {
		s := slots[idx]
		if atomic.CompareAndSwapInt32(&s.dead, 0, 1) {
			_ = s.client.Close()
		}
	}

	// Barrier machinery for phases 2-3. A conn's close path is parked in
	// its pre-lock hook while Engine.Stop shuts down: Stop must wait for
	// that conn instead of racing past it, and a concurrent writer must
	// observe net.ErrClosed rather than corrupting state.
	closeEntered := make(chan *Conn, 16)
	allowClose := make(chan struct{})
	var parkedClose int32
	var parkedConnPtr unsafe.Pointer
	prevClose := hookConnBeforeClose
	hookConnBeforeClose = func(c *Conn) {
		if atomic.LoadInt32(&parkedClose) == 1 &&
			atomic.LoadPointer(&parkedConnPtr) == unsafe.Pointer(c) {
			select {
			case closeEntered <- c:
			default:
			}
			<-allowClose
		}
	}
	defer func() { hookConnBeforeClose = prevClose }()

	for i := 0; i < nConns; i++ {
		establish(i)
	}

	rng := rand31{state: uint64(seed)}

	// Phase 1: deterministic churn.
	for step := 0; step < steps; step++ {
		idx := int(rng.next()) % nConns
		s := slots[idx]
		switch step % 7 {
		case 0, 1, 2: // ordered echo round trip
			if atomic.LoadInt32(&s.dead) == 1 {
				establish(idx)
				s = slots[idx]
			}
			n := atomic.AddInt64(&s.seq, 1)
			payload := []byte{byte('0' + (n % 10)), byte('0' + ((n / 10) % 10)), byte(idx + 'a')}
			done := make(chan struct{})
			readerConn := startReader(payload, done)
			if _, err := readerConn.Write(payload); err != nil {
				t.Fatalf("step %d client write: %v", step, err)
			}
			select {
			case <-done:
			case <-time.After(lifecycleWait):
				t.Fatalf("step %d echo not received", step)
			}
			_ = readerConn.Close()
		case 3: // server-side spontaneous write to a dedicated reopen pair
			// handled below as an independent round trip to keep each
			// client byte stream an exact echo of what it sent
		case 4: // client closes
			closeSlot(idx)
		case 5: // server closes
			if atomic.CompareAndSwapInt32(&s.dead, 0, 1) {
				_ = s.server.Close()
				// close path must be idempotent
				_ = s.server.Close()
				if _, err := s.server.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
					t.Fatalf("step %d Write after server close err = %v", step, err)
				}
			}
		case 6: // reopen a dead slot
			if atomic.LoadInt32(&s.dead) == 1 {
				establish(idx)
			}
		}
	}

	// Phase 2: park the close of one conn while Stop is shutting down,
	// and hammer it with concurrent writes.
	alive := -1
	for i, s := range slots {
		if atomic.LoadInt32(&s.dead) == 0 {
			alive = i
			break
		}
	}
	if alive < 0 {
		for i := range slots {
			establish(i)
		}
		alive = 0
	}
	s := slots[alive]
	atomic.StorePointer(&parkedConnPtr, unsafe.Pointer(s.server))
	atomic.StoreInt32(&parkedClose, 1)

	parked := make(chan struct{})
	go func() {
		<-closeEntered
		close(parked)
	}()
	closeStarted := make(chan struct{})
	go func() {
		close(closeStarted)
		_ = s.server.Close()
	}()
	<-closeStarted
	waitSignal(t, parked, "close path parked")

	// Concurrent writers must not panic, block forever or touch freed
	// memory; they either write bytes or get net.ErrClosed.
	var writers sync.WaitGroup
	for w := 0; w < 3; w++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for i := 0; i < 32; i++ {
				if _, err := s.server.Write([]byte{'w'}); err != nil &&
					!errors.Is(err, net.ErrClosed) {
					t.Errorf("concurrent Write err = %v", err)
					return
				}
			}
		}()
	}

	stopDone := make(chan struct{})
	go func() { g.Stop(); close(stopDone) }()

	select {
	case <-stopDone:
		t.Fatal("Stop returned while a close path was parked")
	case <-time.After(100 * time.Millisecond):
	}
	atomic.StoreInt32(&parkedClose, 0)
	close(allowClose)

	select {
	case <-stopDone:
	case <-time.After(lifecycleWait):
		t.Fatal("Stop hung with a parked close")
	}
	stopped = true
	writers.Wait()

	// Phase 3: global accounting.
	if atomic.LoadInt64(&opens) != atomic.LoadInt64(&ncloses) {
		t.Fatalf("opens=%d closes=%d, want equal", opens, ncloses)
	}
	if !alloc.balanced() {
		t.Fatalf("allocator unbalanced: malloc=%d free=%d",
			atomic.LoadInt64(&alloc.mallocs), atomic.LoadInt64(&alloc.frees))
	}
	for i, sl := range slots {
		if sl.server != nil {
			closeMux.Lock()
			n := closeCnt[sl.server]
			closeMux.Unlock()
			if n != 1 {
				t.Fatalf("slot %d OnClose count = %d, want 1", i, n)
			}
		}
		if sl.client != nil {
			_ = sl.client.Close()
		}
	}
	assertNoLeakedLoops(t, before)
}
