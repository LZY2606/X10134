// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import (
	"io"
	"net"
	"sync"
	"syscall"
	"testing"
)

// Scenario S2 (unix): the write queue already holds multiple cached
// buffers when the peer closes without reading. Pinned stages:
//  1. OnOpen writes big chunks until c.writeList contains >= 3 buffers,
//     proving the queue really accumulated before the close event.
//  2. Only then the peer closes.
//  3. The poller observes EOF: queued buffers are released exactly once
//     through the allocator, OnClose fires once with a peer-close error
//     category, no event is published twice.
func TestLifecyclePeerCloseWithQueuedBuffers(t *testing.T) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	alloc := newTrackingAllocator(t)
	counters := newConnCounters()
	queued := make(chan struct{})
	serverClosed := make(chan struct{})
	var (
		closeErr   error
		errMu      sync.Mutex
		serverConn *Conn
		scMu       sync.Mutex
		queuedOnce sync.Once
	)

	g, _ := newListeningEngine(t, Config{
		Name:          "lc-s2",
		NPoller:       1,
		BodyAllocator: alloc,
	}, func(g *Engine) {
		g.OnOpen(func(c *Conn) {
			counters.markOpen(c)
			scMu.Lock()
			serverConn = c
			scMu.Unlock()
			// Tiny send buffer so writes queue quickly on loopback.
			_ = c.SetWriteBuffer(4 * 1024)
			chunk := make([]byte, 128*1024)
			for i := range chunk {
				chunk[i] = byte(i)
			}
			// Writes run inline until the send-q fills; afterwards each
			// residual lands in the allocator-backed writeList.
			for i := 0; i < 32; i++ {
				_, err := c.Write(chunk)
				if err != nil && err != syscall.EAGAIN && !isLocalClosed(err) {
					t.Errorf("queue fill write %d: %v", i, err)
					return
				}
				c.mux.Lock()
				nList := len(c.writeList)
				c.mux.Unlock()
				if nList >= 3 {
					queuedOnce.Do(func() { close(queued) })
					break
				}
			}
		})
		g.OnData(func(c *Conn, data []byte) {
			t.Errorf("OnData must not fire on a peer that never sends")
		})
		g.OnClose(func(c *Conn, err error) {
			counters.markClose(c)
			errMu.Lock()
			closeErr = err
			errMu.Unlock()
			close(serverClosed)
		})
	})

	peer, err := net.Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatal(err)
	}
	if tc, ok := peer.(*net.TCPConn); ok {
		// Small receive window and never read: the server send-q backs
		// up within the OnOpen write loop.
		_ = tc.SetReadBuffer(4 * 1024)
	}

	waitSignal(t, queued, "write queue accumulated")

	// Peer closes with unread data pending: server gets EOF or reset.
	_ = peer.Close()

	waitSignal(t, serverClosed, "OnClose")
	errMu.Lock()
	gotErr := closeErr
	errMu.Unlock()
	if gotErr == nil || (!isPeerClosedError(gotErr) && gotErr != io.EOF) {
		t.Fatalf("OnClose err = %v, want peer-close category (EOF/reset)", gotErr)
	}

	scMu.Lock()
	c := serverConn
	scMu.Unlock()
	if n := counters.closeCount(c); n != 1 {
		t.Fatalf("OnClose fired %d times, want 1", n)
	}
	counters.assertOnceEach(t)
	// Queued buffers are returned to the allocator exactly once:
	// no double free, no leak.
	alloc.assertBalanced(t)
}
