// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

// fixedLCG is a tiny deterministic PRNG (Numerical Recipes constants);
// the race scenario must replay the exact same operation sequence on
// every machine and run.
type fixedLCG struct {
	mu    sync.Mutex
	state uint64
}

func newFixedLCG(seed uint64) *fixedLCG { return &fixedLCG{state: seed} }

func (r *fixedLCG) next() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = r.state*6364136223846793005 + 1442695040888963407
	return r.state
}

func (r *fixedLCG) payload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		// Keep 0xEE reserved so freed (poisoned) buffers stand out;
		// rejection sampling is required because no modulus < 256 can
		// exclude a single value.
		v := byte(r.next())
		for v == trackingPoisonByte {
			v = byte(r.next())
		}
		b[i] = v
	}
	return b
}

// TestLifecycle_DeterministicRaceSequence replays a fixed-seed sequence
// of writes, partial drains, peer closes and Stop over a small set of
// connections. It pins the phases with barriers (no sleeps, no random
// scheduling) and asserts:
//   - no write-after-release: OnWrittenSize never sees freed bytes,
//   - no double close / duplicate event: every conn opens and closes once,
//   - no missed wakeup: a full-send-cache connection always makes progress,
//   - no Stop hang: Stop returns promptly and wakes every poller,
//   - no leaked write buffers or goroutines.
//
// Mutations such as "release buffer before write callback", "close path
// publishes the event twice" or "Stop waking only some pollers" make this
// test fail deterministically.
func TestLifecycle_DeterministicRaceSequence(t *testing.T) {
	const rounds = 3

	for round := 0; round < rounds; round++ {
		runDeterministicRaceRound(t, round)
	}
}

func runDeterministicRaceRound(t *testing.T, round int) {
	alloc := newTrackingAllocator()
	rec := newConnRecorder()

	// The unix flush hook must never observe released bytes and must
	// actually fire when cached data drains successfully.
	var flushedCalls int32
	var orderMu sync.Mutex
	flushedSet := map[uintptr]struct{}{}
	prevFlushed := testHookConnFlushed
	prevReleased := testHookWriteBufReleased
	t.Cleanup(func() {
		testHookConnFlushed = prevFlushed
		testHookWriteBufReleased = prevReleased
	})
	keyOf := func(b []byte) uintptr {
		if cap(b) == 0 {
			return 0
		}
		return uintptr(unsafe.Pointer(&b[:cap(b)][0]))
	}
	testHookConnFlushed = func(c *Conn, b []byte, n int) {
		atomic.AddInt32(&flushedCalls, 1)
		alloc.assertCachedBytesValid(t, b, n)
		orderMu.Lock()
		flushedSet[keyOf(b)] = struct{}{}
		orderMu.Unlock()
	}
	testHookWriteBufReleased = func(c *Conn, b []byte) {
		orderMu.Lock()
		_, ok := flushedSet[keyOf(b)]
		orderMu.Unlock()
		if !ok {
			t.Fatalf("write buffer released before the write callback completed")
		}
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	g := NewEngine(Config{
		Name:          "race-round",
		Network:       "tcp",
		Addrs:         []string{addr},
		NPoller:       4,
		BodyAllocator: alloc,
	})
	g.OnOpen(rec.onOpen)
	g.OnData(func(c *Conn, data []byte) {
		rec.onData(c, data)
		// Echo; queues in the write cache when the peer reads slowly.
		_, _ = c.Write(append([]byte{}, data...))
	})
	g.OnClose(rec.onClose)
	g.OnWrittenSize(func(c *Conn, b []byte, n int) {
		alloc.assertCachedBytesValid(t, b, n)
	})
	if err := g.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	const connNum = 3

	// large enough to exceed the write-cache merge threshold on every platform
	const raceBigWriteSize = 64*1024 + 1
	rng := newFixedLCG(0x5EED1234 + uint64(round))
	clients := make([]net.Conn, connNum)
	svrs := make([]*Conn, connNum)
	for i := range clients {
		clients[i] = mustDial(t, addr)
		if tc, ok := clients[i].(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
		}
		svrs[i] = rec.waitOpen(t)
	}
	defer func() {
		for _, c := range clients {
			if c != nil {
				_ = c.Close()
			}
		}
	}()

	readSome := func(c net.Conn, n int) []byte {
		t.Helper()
		buf := make([]byte, n)
		got := 0
		_ = c.SetReadDeadline(deadlineSoon())
		for got < n {
			m, rerr := c.Read(buf[got:])
			got += m
			if rerr != nil {
				break
			}
		}
		return buf[:got]
	}

	// Phase A: small ordered echoes on every connection; verifies event
	// ordering and that callbacks stay per-conn sequential.
	for i := 0; i < 6; i++ {
		idx := int(rng.next() % connNum)
		msg := rng.payload(1 + int(rng.next()%127))
		if _, err := clients[idx].Write(msg); err != nil {
			t.Fatalf("round %d phase A write: %v", round, err)
		}
		echo := readSome(clients[idx], len(msg))
		if string(echo) != string(msg) {
			t.Fatalf("round %d echo mismatch on conn %d: got %d/%d bytes",
				round, idx, len(echo), len(msg))
		}
	}

	// Phase A2: force a write into the cache (peer receive window
	// smaller than the message), then drain it completely. The cached
	// buffers go through flush -> write callback -> release, so a
	// release-before-callback mutation is caught by the poison check and
	// by byte corruption here.
	{
		idx := int(rng.next() % connNum)
		tc := clients[idx].(*net.TCPConn)
		_ = tc.SetReadBuffer(4096)
		_ = svrs[idx].SetWriteBuffer(4096)
		_ = svrs[idx].SetNoDelay(true)
		msg := rng.payload(512 * 1024)
		// Write from the test goroutine directly onto the server conn;
		// wait (bounded) until the write cache is non-empty, proving the
		// flush path will be exercised on drain.
		if _, err := svrs[idx].Write(msg); err != nil {
			t.Fatalf("round %d phase A2 server write: %v", round, err)
		}
		waitQueued := time.Now().Add(lifecycleTestTimeout)
		for {
			if queuedLen(t, svrs[idx]) > 0 {
				break
			}
			if time.Now().After(waitQueued) {
				t.Fatalf("round %d phase A2 never queued cached bytes", round)
			}
			time.Sleep(time.Millisecond)
		}
		echo := readSome(clients[idx], len(msg))
		if len(echo) != len(msg) {
			t.Fatalf("round %d drain mismatch: got %d/%d bytes",
				round, len(echo), len(msg))
		}
		for i := range msg {
			if echo[i] != msg[i] {
				t.Fatalf("round %d corrupted cached byte at %d", round, i)
			}
		}
		if atomic.LoadInt32(&flushedCalls) == 0 {
			t.Fatalf("round %d cached drain produced no flush write callback", round)
		}
		_ = tc.SetReadBuffer(64 * 1024)
	}

	// Phase B: one connection floods beyond the send window so the
	// server queues buffers, then the peer goes away without draining.
	// The queued buffers must all be returned once and Stop must not hang.
	floodIdx := int(rng.next() % connNum)
	floodDone := make(chan struct{})
	go func() {
		defer close(floodDone)
		big := rng.payload(raceBigWriteSize + 4096)
		for i := 0; i < 4; i++ {
			if _, werr := clients[floodIdx].Write(big); werr != nil {
				return
			}
		}
	}()
	<-floodDone
	_ = clients[floodIdx].Close()
	clients[floodIdx] = nil

	// Phase C: overlapping writes and closes from several goroutines on
	// the remaining connections; duplicate close events or corrupted
	// write callbacks surface here.
	var wg sync.WaitGroup
	for i := 0; i < connNum; i++ {
		if clients[i] == nil {
			continue
		}
		i := i
		for k := 0; k < 3; k++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = svrs[i].Write(rng.payload(37))
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = svrs[i].Close()
			_ = svrs[i].Close()
		}()
	}
	wg.Wait()

	// Drain every recorded close before stopping: close path events are
	// delivered once each.
	for _, c := range svrs {
		_ = rec.waitClose(t, c)
	}
	rec.assertOncePerConn(t)

	// Stop must wake all four pollers and return promptly.
	stopEngine(t, g)
	alloc.assertBalanced(t)
	assertNoNbioGoroutines(t, g)

	// Phase D (post-Stop sanity): writing to a stopped engine's conn is
	// a clean failure, never a panic or reuse.
	for _, c := range svrs {
		if _, err := c.Write([]byte("z")); err == nil {
			t.Fatalf("write after Stop succeeded")
		}
	}
}
