// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio_test

import (
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lesismal/nbio"
)

// smallSndbufConn shrinks the kernel send buffer so a bounded write fills
// the peer's receive window and forces nbio to queue multiple caches.
func smallSndbufConn(t *testing.T, c net.Conn) {
	t.Helper()
	if tc, ok := c.(*net.TCPConn); ok {
		if err := tc.SetWriteBuffer(1024); err != nil {
			t.Logf("set write buffer: %v", err)
		}
		_ = tc.SetNoDelay(true)
	}
}

func drain(t *testing.T, c net.Conn, total int) {
	t.Helper()
	buf := make([]byte, 64*1024)
	remaining := total
	for remaining > 0 {
		n, err := c.Read(buf)
		remaining -= n
		if err != nil {
			if errors.Is(err, io.EOF) && remaining <= 0 {
				return
			}
			return
		}
	}
}

// TestLifecycle_QueuedBuffersPeerHalfClose: the write cache holds multiple
// buffers when the peer half-closes (sends FIN) and keeps draining. All
// queued data must be delivered, every cached block released exactly once,
// the write callback must never see a freed block, and onClose fires once
// with io.EOF.
func TestLifecycle_QueuedBuffersPeerHalfClose(t *testing.T) {
	alloc := newTrackingAllocator()
	var (
		openReady   = make(chan *nbio.Conn, 1)
		closeCount  int32
		writtenSize int64
		closeErrMu  sync.Mutex
		closeErr    error
	)

	g := nbio.NewEngine(nbio.Config{
		Network:       "tcp",
		NPoller:       1,
		BodyAllocator: alloc,
	})
	g.OnOpen(func(c *nbio.Conn) { openReady <- c })
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, err error) {
		closeErrMu.Lock()
		closeErr = err
		closeErrMu.Unlock()
		atomic.AddInt32(&closeCount, 1)
	})
	g.OnWrittenSize(func(c *nbio.Conn, b []byte, n int) {
		alloc.checkCallback(b)
		atomic.AddInt64(&writtenSize, int64(n))
	})
	addr := startEphemeral(t, g)

	client := dialRaw(t, addr)
	defer client.Close()
	if tc, ok := client.(*net.TCPConn); ok {
		if err := tc.SetReadBuffer(1024); err != nil {
			t.Logf("set client read buffer: %v", err)
		}
	}

	srv := <-openReady

	// Client advertises a tiny receive window; the server fills its send
	// queue with blocks larger than the 64KiB cache-coalescing limit, so at
	// least two cached blocks exist while the peer is still connected.
	total := 4 * 1024 * 1024
	payload := bytesRepeating(total, 0xAB)
	go func() {
		_, _ = srv.Write(payload)
	}()

	// Wait until multiple blocks were allocated for the cache, proving the
	// scenario reached the "multiple queued buffers" state.
	waitFor(t, func() bool { return atomic.LoadInt64(&alloc.mallocN) >= 2 }, "multiple cached write blocks")

	// Peer half-closes its write side (sends FIN) while still reading.
	if tc, ok := client.(*net.TCPConn); ok {
		if err := tc.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}

	// Drain everything the server queued.
	drain(t, client, total)

	// Server observes EOF after the FIN; stop and collect.
	done := stopEngineAsync(t, g)
	waitSignal(t, done, "engine stop")

	if got := atomic.LoadInt32(&closeCount); got != 1 {
		t.Fatalf("onClose count = %d, want 1", got)
	}
	closeErrMu.Lock()
	gotErr := closeErr
	closeErrMu.Unlock()
	if !errors.Is(gotErr, io.EOF) {
		t.Fatalf("close error = %v, want io.EOF after peer half-close", gotErr)
	}
	for _, msg := range alloc.errors() {
		t.Errorf("allocator: %s", msg)
	}
	alloc.mu.Lock()
	leaked := len(alloc.live)
	alloc.mu.Unlock()
	if leaked != 0 {
		t.Fatalf("cached write blocks leaked: %d", leaked)
	}
	if got := atomic.LoadInt64(&writtenSize); got != int64(total) {
		t.Fatalf("written size = %d, want %d", got, total)
	}
}

// TestLifecycle_QueuedBuffersPeerAbort: the write cache holds multiple
// buffers when the peer closes hard (RST). The cached blocks must still be
// released exactly once, the close error belongs to the peer-close family,
// and onClose fires once.
func TestLifecycle_QueuedBuffersPeerAbort(t *testing.T) {
	alloc := newTrackingAllocator()
	var (
		openReady  = make(chan *nbio.Conn, 1)
		closeCount int32
		closeErrMu sync.Mutex
		closeErr   error
	)

	g := nbio.NewEngine(nbio.Config{
		Network:       "tcp",
		NPoller:       1,
		BodyAllocator: alloc,
	})
	g.OnOpen(func(c *nbio.Conn) { openReady <- c })
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, err error) {
		closeErrMu.Lock()
		closeErr = err
		closeErrMu.Unlock()
		atomic.AddInt32(&closeCount, 1)
	})
	g.OnWrittenSize(func(c *nbio.Conn, b []byte, n int) {
		alloc.checkCallback(b)
	})
	addr := startEphemeral(t, g)

	client := dialRaw(t, addr)
	srv := <-openReady

	// Enable linger-0 so client.Close() sends RST with unread data pending.
	if tc, ok := client.(*net.TCPConn); ok {
		setLinger0(t, tc)
	}

	total := 4 * 1024 * 1024
	go func() { _, _ = srv.Write(bytesRepeating(total, 0xCD)) }()
	waitFor(t, func() bool { return atomic.LoadInt64(&alloc.mallocN) >= 2 }, "multiple cached write blocks")

	// Hard close without draining: RST.
	_ = client.Close()

	done := stopEngineAsync(t, g)
	waitSignal(t, done, "engine stop")

	if got := atomic.LoadInt32(&closeCount); got != 1 {
		t.Fatalf("onClose count = %d, want 1", got)
	}
	closeErrMu.Lock()
	gotErr := closeErr
	closeErrMu.Unlock()
	if !isPeerCloseError(gotErr) {
		t.Fatalf("close error = %v, want EOF/reset/broken-pipe after peer abort", gotErr)
	}
	for _, msg := range alloc.errors() {
		t.Errorf("allocator: %s", msg)
	}
	alloc.mu.Lock()
	leaked := len(alloc.live)
	alloc.mu.Unlock()
	if leaked != 0 {
		t.Fatalf("cached write blocks leaked after abort: %d", leaked)
	}
}

func bytesRepeating(n int, b byte) []byte {
	s := make([]byte, n)
	for i := range s {
		s[i] = b
	}
	return s
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(lifecycleWait)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timeout waiting for %s", what)
}
