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
	"sync/atomic"
	"syscall"
	"testing"
)

// peerClosedByRemote reports whether err belongs to the documented
// "peer closed the connection" error family (orderly EOF, reset or
// broken pipe). Local timeouts/overflow must never appear here.
func peerClosedByRemote(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, io.EOF) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, net.ErrClosed)
}

// Scenario 2: several buffers are already queued in the Conn's write
// cache (multiple writeList entries) when the peer closes. The first
// flush attempt is parked by a test hook so writes are forced to queue;
// then the peer closes and the parked flush is released. Every queued
// buffer must be returned to the allocator exactly once, the write
// callback must never observe released bytes, and OnClose must fire once
// with a peer-close class error.
func TestLifecycle_QueuedWriteBuffersOnPeerClose(t *testing.T) {
	alloc := newTrackingAllocator()

	flushParked := make(chan struct{})
	releaseFlush := make(chan struct{})
	var parked int32
	var parkedConn atomic.Value // *Conn

	prevFlush := testHookConnFlush
	prevFlushed := testHookConnFlushed
	t.Cleanup(func() {
		testHookConnFlush = prevFlush
		testHookConnFlushed = prevFlushed
	})
	testHookConnFlush = func(c *Conn) {
		if pc, ok := parkedConn.Load().(*Conn); ok && pc == c &&
			atomic.CompareAndSwapInt32(&parked, 0, 1) {
			close(flushParked)
			<-releaseFlush
		}
	}
	testHookConnFlushed = func(c *Conn, b []byte, n int) {
		alloc.assertCachedBytesValid(t, b, n)
	}

	g, addr, rec := newLifecycleEngine(t, 1, alloc, lifecycleHooks{})
	client := mustDial(t, addr)
	defer func() { _ = client.Close() }()

	svr := rec.waitOpen(t)
	parkedConn.Store(svr)

	// The client will abandon the connection with unread server data;
	// linger 0 makes its close deterministically send RST instead of
	// relying on the kernel's RST-vs-FIN scheduling.
	if tc, ok := client.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}

	// Keep the peer's receive window tiny so server writes are forced
	// into the write cache.
	if tc, ok := client.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
		_ = tc.SetNoDelay(true)
	}
	_ = svr.SetWriteBuffer(4096)
	_ = svr.SetNoDelay(true)

	// Push chunks larger than the write-cache merge threshold so at
	// least two distinct buffers are queued. The poller's first flush is
	// parked by the hook once queued data is observed.
	go func() {
		chunk := make([]byte, maxWriteCacheOrFlushSize+1)
		for i := range chunk {
			chunk[i] = 0xAB
		}
		for i := 0; i < 4; i++ {
			if _, err := svr.Write(chunk); err != nil {
				return
			}
		}
	}()
	waitSignal(t, flushParked, "flush parked with queued writes")

	svr.mux.Lock()
	queued := len(svr.writeList)
	svr.mux.Unlock()
	if queued < 2 {
		close(releaseFlush)
		t.Fatalf("expected at least 2 queued write buffers, got %d", queued)
	}

	// Peer closes with unread data still in flight: the next flush/write
	// must surface a peer-close error, not silently succeed forever.
	if err := client.Close(); err != nil {
		t.Fatalf("peer close: %v", err)
	}
	close(releaseFlush)

	err := rec.waitClose(t, svr)
	if !peerClosedByRemote(err) {
		t.Fatalf("OnClose err = %v, want EOF/ECONNRESET/EPIPE class", err)
	}
	rec.assertOncePerConn(t)
	alloc.assertBalanced(t)

	stopEngine(t, g)
	assertNoNbioGoroutines(t, g)
}
