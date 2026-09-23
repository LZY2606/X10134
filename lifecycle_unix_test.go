// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// queuedCount returns the number of pending toWrite entries for the conn.
func queuedCount(c *Conn) int {
	c.mux.Lock()
	n := len(c.writeList)
	c.mux.Unlock()
	return n
}

// fillUntilQueued writes 64KiB chunks until at least n toWrite entries are
// queued (the peer is not reading) or the deadline expires.
func fillUntilQueued(t *testing.T, c *Conn, n int) int64 {
	t.Helper()
	chunk := make([]byte, 64*1024)
	var total int64
	deadline := time.Now().Add(lifecycleWait)
	for time.Now().Before(deadline) {
		nw, err := c.Write(chunk)
		if err != nil {
			t.Fatalf("fill write: %v", err)
		}
		total += int64(nw)
		if queuedCount(c) >= n {
			return total
		}
	}
	t.Fatalf("only %v buffers queued, want %v", queuedCount(c), n)
	return total
}

func setSmallSockBuffers(t *testing.T, conn net.Conn) {
	t.Helper()
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("not tcp conn: %T", conn)
	}
	// Small OS buffers make the send queue fill deterministically on loopback.
	_ = tc.SetReadBuffer(4096)
	_ = tc.SetWriteBuffer(4096)
}

// Scenario 2: multiple buffers are already queued in the write list when
// the peer closes. Every cached buffer must be returned once, OnClose must
// fire exactly once with a peer-close category error, and Stop must return.
func TestLifecycleUnixQueuedBuffersPeerClose(t *testing.T) {
	h := newHarness(t)
	h.hooks = &testHooks{
		beforeOnOpen: func(c *Conn) {
			_ = c.SetWriteBuffer(4096)
		},
	}
	h.start()

	raw, c := h.dialServer()
	setSmallSockBuffers(t, raw)

	fillUntilQueued(t, c, 3)
	_ = raw.Close()

	got := h.waitClose(c)
	h.assertSingleClose(c)
	assertCloseErrCategory(t, got)
	h.assertBalanced()
	h.stopAndClean()
}

// Missed-wakeup variant: fill the queue with the peer paused, then drain
// the peer and let the poller EVFILT_WRITE/EPOLLOUT flush every buffer.
// A missing wakeup leaves the buffer cached forever and this times out.
func TestLifecycleUnixQueuedBuffersDrainAndWakeup(t *testing.T) {
	h := newHarness(t)
	h.hooks = &testHooks{
		beforeOnOpen: func(c *Conn) {
			_ = c.SetWriteBuffer(4096)
		},
	}
	h.start()

	raw, c := h.dialServer()
	setSmallSockBuffers(t, raw)

	total := fillUntilQueued(t, c, 3)

	drained := make(chan struct{})
	go func() {
		var got int64
		buf := make([]byte, 64*1024)
		for got < total {
			n, err := raw.Read(buf)
			got += int64(n)
			if err != nil {
				return
			}
		}
		close(drained)
	}()

	select {
	case <-drained:
	case <-time.After(lifecycleWait):
		t.Fatalf("queued data not flushed: %v buffers stuck", queuedCount(c))
	}

	// After the flush the write list is empty again.
	deadline := time.Now().Add(lifecycleWait)
	for time.Now().Before(deadline) {
		if queuedCount(c) == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if queuedCount(c) != 0 {
		t.Fatalf("write list not drained: %v", queuedCount(c))
	}

	_ = raw.Close()
	h.waitClose(c)
	h.assertSingleClose(c)
	h.assertBalanced()
	h.stopAndClean()
}

// writerGate parks a foreign writer inside Conn.Write at a deterministic
// point so the test can race Close against the held conn mutex.
type writerGate struct {
	target  *Conn
	enter   chan struct{}
	release chan struct{}
	once    sync.Once
	armed   bool
}

func newWriterGate() *writerGate {
	return &writerGate{enter: make(chan struct{}), release: make(chan struct{})}
}

func (g *writerGate) hook(c *Conn) {
	if g.armed && c == g.target {
		g.once.Do(func() {
			close(g.enter)
			<-g.release
		})
	}
}

// Scenario 3 (unix deterministic interleave): park a foreign writer inside
// Conn.Write while it holds the conn mutex (write cache non-empty), then run
// Close to completion and unblock the writer. The writer must observe
// net.ErrClosed, the queued buffer must be freed once, OnClose fires once,
// and no poisoned byte is ever handed to onWrittenSize.
func TestLifecycleUnixCloseRacesForeignWriter(t *testing.T) {
	h := newHarness(t)
	gate := newWriterGate()
	h.hooks = &testHooks{
		beforeOnOpen: func(c *Conn) {
			_ = c.SetWriteBuffer(4096)
		},
		inWrite: gate.hook,
	}
	h.start()

	raw, c := h.dialServer()
	setSmallSockBuffers(t, raw)
	gate.target = c
	gate.armed = true

	// First writes fill the socket/queue so the parked write finds a
	// non-empty writeList (pure write-cache path, not a direct syscall).
	fillUntilQueued(t, c, 2)

	writeErr := make(chan error, 1)
	go func() {
		_, err := c.Write(make([]byte, 1024))
		writeErr <- err
	}()
	<-gate.enter

	closeErr := make(chan error, 1)
	go func() { closeErr <- c.Close() }()

	// Close must not return while the writer still holds the mutex.
	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-closeErr:
		t.Fatalf("Close completed while writer held mutex: %v", err)
	default:
	}
	close(gate.release)

	select {
	case err := <-writeErr:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("parked write err = %v, want net.ErrClosed", err)
		}
	case <-time.After(lifecycleWait):
		t.Fatalf("parked writer did not return")
	}

	select {
	case <-closeErr:
	case <-time.After(lifecycleWait):
		t.Fatalf("Close did not return after writer released")
	}

	h.waitClose(c)
	h.assertSingleClose(c)
	h.assertBalanced()
	if queuedCount(c) != 0 {
		t.Fatalf("write list not cleaned: %v", queuedCount(c))
	}

	_ = raw.Close()
	h.stopAndClean()
}

// ---- framed traffic for the scripted race scenario ----
//
// Every write is a frame: 2-byte big-endian length n, 1 color byte, then n
// payload bytes all equal to color. Corruption (poison 0xDD), frame tearing
// across connections, and out-of-band bytes all fail parsing.

const maxFrameLen = 2048

func buildFrame(color byte, n int) []byte {
	if n > maxFrameLen {
		n = maxFrameLen
	}
	b := make([]byte, 3+n)
	binary.BigEndian.PutUint16(b[:2], uint16(n))
	b[2] = color
	for i := 0; i < n; i++ {
		b[3+i] = color
	}
	return b
}

// frameReader parses frames off a peer conn until EOF.
type frameReader struct {
	conn   net.Conn
	t      *testing.T
	buf    []byte
	frames int64
}

func (r *frameReader) run(stop <-chan struct{}) {
	tmp := make([]byte, 4096)
	for {
		n, err := r.conn.Read(tmp)
		r.buf = append(r.buf, tmp[:n]...)
		r.parse()
		if err != nil {
			return
		}
		select {
		case <-stop:
			return
		default:
		}
	}
}

func (r *frameReader) parse() {
	for {
		if len(r.buf) < 3 {
			return
		}
		n := int(binary.BigEndian.Uint16(r.buf[:2]))
		color := r.buf[2]
		if n == 0 {
			r.t.Errorf("zero-length frame (corruption)")
			r.buf = r.buf[3:]
			continue
		}
		if len(r.buf) < 3+n {
			return
		}
		for _, b := range r.buf[3 : 3+n] {
			if b != color {
				if b == lifecyclePoison {
					r.t.Errorf("frame payload contains poison byte (write-after-free)")
				} else {
					r.t.Errorf("frame payload color mismatch (cross-write/tear): want %d got %d", color, b)
				}
			}
		}
		r.frames++
		r.buf = r.buf[3+n:]
	}
}

// Repeatable, fixed-seed race: a small set of connections runs a scripted
// operation log that interleaves foreign writes, queued flush, peer close,
// engine-side close, fd reuse (reconnect) and Stop. Deterministic barriers
// pin the interesting interleavings instead of relying on timing.
func TestLifecycleUnixScriptedRace(t *testing.T) {
	h := newHarness(t)
	h.npoller = 3
	h.start()

	// onWrittenSize must never see a poisoned byte either; the buffer is
	// still live here, so this complements the peer-side parser.
	h.g.OnWrittenSize(func(c *Conn, b []byte, n int) {
		for _, x := range b[:n] {
			if x == lifecyclePoison {
				t.Errorf("onWrittenSize saw poison byte (free before write callback)")
				return
			}
		}
	})

	const nslots = 3
	type slot struct {
		mu         sync.Mutex
		raw        net.Conn
		c          *Conn
		reader     *frameReader
		readerStop chan struct{}
		live       bool
	}
	slots := make([]*slot, nslots)
	for i := range slots {
		slots[i] = &slot{}
	}

	connect := func(i int, pauseReader bool) {
		s := slots[i]
		raw, c := h.dialServer()
		setSmallSockBuffers(t, raw)
		c.mux.Lock()
		_ = c.SetWriteBuffer(4096)
		c.mux.Unlock()

		s.mu.Lock()
		s.raw = raw
		s.c = c
		s.live = true
		s.readerStop = make(chan struct{})
		s.reader = &frameReader{conn: raw, t: t, buf: make([]byte, 0, 8192)}
		if !pauseReader {
			go s.reader.run(s.readerStop)
		}
		s.mu.Unlock()
	}
	closeSlot := func(i int) {
		s := slots[i]
		s.mu.Lock()
		if !s.live {
			s.mu.Unlock()
			return
		}
		s.live = false
		raw := s.raw
		stop := s.readerStop
		s.mu.Unlock()
		_ = raw.Close()
		if stop != nil {
			close(stop)
		}
	}

	for i := range slots {
		connect(i, false)
	}

	// ---- Phase A: queue several frames with the peer paused, then resume
	// the peer and require every frame delivered via write-event wakeup. ----
	// Pause the slot-0 reader by not letting it run: reconnect slot 0 with
	// a paused reader, fill the write list, then start draining.
	closeSlot(0)
	h.waitClose(slots[0].c)
	connect(0, true)
	const wantFramesA = 24
	queued := false
	for k := 0; k < wantFramesA; k++ {
		if _, err := slots[0].c.Write(buildFrame(byte(1+k%200), 1024)); err != nil {
			t.Fatalf("phase A write: %v", err)
		}
		if !queued && queuedCount(slots[0].c) >= 3 {
			queued = true
		}
	}
	if !queued {
		t.Fatalf("expected queued buffers, got %v", queuedCount(slots[0].c))
	}
	// Resume draining: all queued and directly-written frames must arrive.
	go slots[0].reader.run(slots[0].readerStop)
	flushed := make(chan struct{})
	go func() {
		deadline := time.Now().Add(lifecycleWait)
		for time.Now().Before(deadline) {
			if atomicLoadFrames(slots[0].reader) >= wantFramesA && queuedCount(slots[0].c) == 0 {
				close(flushed)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	select {
	case <-flushed:
	case <-time.After(lifecycleWait):
		t.Fatalf("queued frames not delivered (missed wakeup), frames=%v queued=%v",
			atomicLoadFrames(slots[0].reader), queuedCount(slots[0].c))
	}

	// ---- Phase B: foreign write and close fixed interleave on slot 1 ----
	s1 := slots[1]
	gate := newWriterGate()
	h.g.testHooks.inWrite = func(c *Conn) {
		if c == s1.c {
			gate.hook(c)
		}
	}
	gate.target = s1.c
	gate.armed = true
	for k := 0; k < 2; k++ {
		if _, err := s1.c.Write(buildFrame(byte(10+k), 2048)); err != nil {
			t.Fatalf("phase B fill: %v", err)
		}
	}
	writeRet := make(chan error, 1)
	go func() {
		_, err := s1.c.Write(buildFrame(33, 256))
		writeRet <- err
	}()
	<-gate.enter
	go func() { _ = s1.c.Close() }()
	time.Sleep(20 * time.Millisecond)
	close(gate.release)
	select {
	case err := <-writeRet:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("phase B write err = %v, want ErrClosed", err)
		}
	case <-time.After(lifecycleWait):
		t.Fatalf("phase B writer stuck")
	}
	h.waitClose(s1.c)
	closeSlot(1)
	h.assertSingleClose(s1.c)
	h.g.testHooks.inWrite = nil

	// ---- Phase C: reconnect slot 1 (fd reuse), exercise close path ----
	connect(1, false)
	framesDelivered := make(chan struct{})
	go func() {
		want := int64(8)
		deadline := time.Now().Add(lifecycleWait)
		for time.Now().Before(deadline) {
			if atomicLoadFrames(slots[1].reader) >= want {
				close(framesDelivered)
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	for k := 0; k < 8; k++ {
		if _, err := slots[1].c.Write(buildFrame(byte(40+k), 64)); err != nil {
			t.Fatalf("phase C write: %v", err)
		}
	}
	select {
	case <-framesDelivered:
	case <-time.After(lifecycleWait):
		t.Fatalf("reconnected conn frames lost (fd reuse mix-up)")
	}

	// ---- Phase D: fixed-seed op log across all slots ----
	seq := fixedSequence(7)
	for step := 0; step < 200; step++ {
		i := int(seq() % nslots)
		s := slots[i]
		s.mu.Lock()
		live := s.live
		c := s.c
		s.mu.Unlock()
		if !live {
			connect(i, false)
			continue
		}
		switch seq() % 5 {
		case 0, 1, 2:
			color := byte(1 + seq()%200)
			n := 64 + int(seq()%1024)
			if _, err := c.Write(buildFrame(color, n)); err != nil {
				// conn may have been closed by a prior op this round.
				assertCloseErrCategory(t, err)
			}
		case 3:
			_ = c.Close()
		case 4:
			go func() { _, _ = c.Write(buildFrame(99, 128)) }()
			_ = c.Close()
		}
	}

	// Close all peer conns to make every engine conn terminate.
	for i := range slots {
		closeSlot(i)
	}

	h.stopAndClean()
}

func atomicLoadFrames(r *frameReader) int64 {
	return atomic.LoadInt64(&r.frames)
}
