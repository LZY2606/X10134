// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio_test

import (
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/lesismal/nbio"
)

// TestLifecycle_CloseDuringOnOpen: while OnOpen is still executing, another
// goroutine requests close. onClose must fire exactly once with a nil close
// cause, a later Write must fail with net.ErrClosed, and Stop must not hang.
func TestLifecycle_CloseDuringOnOpen(t *testing.T) {
	var (
		openStarted  = make(chan struct{})
		connHandover = make(chan *nbio.Conn)
		openReturn   = make(chan struct{})
		closeCount   int32
	)

	g := nbio.NewEngine(nbio.Config{NPoller: 1})
	g.OnOpen(func(c *nbio.Conn) {
		// First conn is this test's; a token channel keeps the gate
		// one-shot and immune to any stray connection.
		select {
		case <-openStarted:
			return
		default:
			close(openStarted)
		}
		connHandover <- c
		<-connHandover // wait until the closer goroutine ran
		n, werr := c.Write([]byte("late"))
		if !errors.Is(werr, net.ErrClosed) {
			t.Errorf("write after raced close: n=%d err=%v, want net.ErrClosed", n, werr)
		}
		close(openReturn)
	})
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, closeErr error) {
		if closeErr != nil {
			t.Errorf("close error = %v, want nil for user-initiated close", closeErr)
		}
		atomic.AddInt32(&closeCount, 1)
	})
	addr := startEphemeral(t, g)

	client := dialRaw(t, addr)
	defer client.Close()

	waitSignal(t, openStarted, "onOpen start")

	// A different goroutine requests close while onOpen has not returned.
	c := <-connHandover
	if err := c.Close(); err != nil {
		t.Fatalf("concurrent close: %v", err)
	}
	connHandover <- c

	waitSignal(t, openReturn, "onOpen return")

	done := stopEngineAsync(t, g)
	waitSignal(t, done, "engine stop")

	if got := atomic.LoadInt32(&closeCount); got != 1 {
		t.Fatalf("onClose count = %d, want 1", got)
	}
}

// TestLifecycle_AsyncWriteThenClose: OnData triggers an asynchronous write
// from another goroutine and closes immediately. The write must either land
// cleanly or fail with net.ErrClosed, never panic or corrupt state, and the
// conn must still close exactly once. Repeated to cover both interleavings.
func TestLifecycle_AsyncWriteThenClose(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		var (
			dataArrived = make(chan struct{}, 1)
			closeCount  int32
			writeErrMu  sync.Mutex
			writeErrs   []error
		)

		g := nbio.NewEngine(nbio.Config{NPoller: 1})
		g.OnOpen(func(c *nbio.Conn) {})
		g.OnData(func(c *nbio.Conn, data []byte) {
			select {
			case dataArrived <- struct{}{}:
			default:
			}
			go func() {
				if _, werr := c.Write(append([]byte{}, data...)); werr != nil &&
					!errors.Is(werr, net.ErrClosed) {
					writeErrMu.Lock()
					writeErrs = append(writeErrs, werr)
					writeErrMu.Unlock()
				}
			}()
			_ = c.Close()
		})
		g.OnClose(func(c *nbio.Conn, closeErr error) {
			atomic.AddInt32(&closeCount, 1)
		})
		addr := startEphemeral(t, g)

		client := dialRaw(t, addr)
		if _, err := client.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		waitSignal(t, dataArrived, "server onData")
		// Drain whatever the raced write delivered, then EOF.
		buf := make([]byte, 64)
		for {
			if _, err := client.Read(buf); err != nil {
				break
			}
		}
		client.Close()

		done := stopEngineAsync(t, g)
		waitSignal(t, done, "engine stop")

		writeErrMu.Lock()
		for _, werr := range writeErrs {
			t.Fatalf("async write returned unexpected error %v", werr)
		}
		writeErrMu.Unlock()
		if got := atomic.LoadInt32(&closeCount); got != 1 {
			t.Fatalf("iter %d: onClose count = %d, want 1", iter, got)
		}
	}
}

// TestLifecycle_AcceptDuringStop: Stop begins while a connection is parked
// inside Accept (before it reaches the poller's addConn). Stop must not hang:
// the in-flight connection is either tracked (then closed by Stop) or
// rejected without being tracked.
func TestLifecycle_AcceptDuringStop(t *testing.T) {
	addr, gl := newGateListener(t)
	defer gl.letGo()

	var (
		openCount  int32
		closeCount int32
	)

	g := nbio.NewEngine(nbio.Config{
		Name:    "lifecycle-accept-stop",
		Network: "tcp",
		NPoller: 1,
	})
	g.Listen = func(network, listenAddr string) (net.Listener, error) {
		return gl, nil
	}
	g.OnOpen(func(c *nbio.Conn) { atomic.AddInt32(&openCount, 1) })
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, err error) {
		atomic.AddInt32(&closeCount, 1)
	})
	_ = addr
	g.Addrs = []string{addr}
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}

	client := dialRaw(t, addr)
	defer client.Close()

	// Wait until the listener goroutine has accepted and is parked.
	waitSignal(t, gl.stalled, "listener accept parked")

	// The gate holds the accepted conn before it reaches addConn; closing
	// the listener proves Stop has entered its shutdown window (listeners
	// are stopped before the conn table is snapshotted).
	stopDone := stopEngineAsync(t, g)
	waitSignal(t, gl.closeObserved, "stop began closing listeners")
	gl.letGo()

	// The in-flight conn now races the snapshot: it must either be tracked
	// (and therefore closed by Stop) or rejected; in both cases Stop has to
	// return, callbacks fire at most once. We do not assert which path won.
	waitSignal(t, stopDone, "engine stop with accept in flight")

	if got := atomic.LoadInt32(&openCount); got > 1 {
		t.Fatalf("onOpen count = %d, want <= 1", got)
	}
	if got := atomic.LoadInt32(&closeCount); got > 1 {
		t.Fatalf("onClose count = %d, want <= 1", got)
	}
}

// TestLifecycle_ReentrantCloseInOnClose: the user OnClose callback calls
// Close on the same conn again. The callback must run once and Stop finish.
func TestLifecycle_ReentrantCloseInOnClose(t *testing.T) {
	var (
		openReady  = make(chan *nbio.Conn, 1)
		closeCount int32
	)

	g := nbio.NewEngine(nbio.Config{NPoller: 1})
	g.OnOpen(func(c *nbio.Conn) { openReady <- c })
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, closeErr error) {
		// Re-entrant close must return immediately, never re-dispatch.
		if err := c.Close(); err != nil {
			t.Errorf("reentrant Close: %v", err)
		}
		if err := c.CloseWithError(errors.New("again")); err != nil {
			t.Errorf("reentrant CloseWithError: %v", err)
		}
		atomic.AddInt32(&closeCount, 1)
	})
	addr := startEphemeral(t, g)

	client := dialRaw(t, addr)
	defer client.Close()

	c := <-openReady
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	done := stopEngineAsync(t, g)
	waitSignal(t, done, "engine stop")
	if got := atomic.LoadInt32(&closeCount); got != 1 {
		t.Fatalf("onClose count = %d, want 1", got)
	}
}

// TestLifecycle_StopWakesAllPollers: with several pollers and live conns
// spread across them, Stop must return: every poller has to be woken, and
// every tracked conn closed.
func TestLifecycle_StopWakesAllPollers(t *testing.T) {
	const nPoller = 4
	const nConn = 12

	var (
		openWg     sync.WaitGroup
		closeCount int32
	)
	openWg.Add(nConn)

	g := nbio.NewEngine(nbio.Config{NPoller: nPoller})
	g.OnOpen(func(c *nbio.Conn) {
		openWg.Done()
	})
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, err error) {
		atomic.AddInt32(&closeCount, 1)
	})
	addr := startEphemeral(t, g)

	clients := make([]net.Conn, nConn)
	for i := 0; i < nConn; i++ {
		clients[i] = dialRaw(t, addr)
		defer clients[i].Close()
	}
	openWg.Wait()

	baseline := runtime.NumGoroutine()
	done := stopEngineAsync(t, g)
	waitSignal(t, done, "stop across all pollers")

	if got := atomic.LoadInt32(&closeCount); got != nConn {
		t.Fatalf("onClose count = %d, want %d", got, nConn)
	}
	for _, client := range clients {
		buf := make([]byte, 1)
		if _, err := client.Read(buf); err == nil {
			t.Fatalf("client conn still readable after engine stop")
		}
	}
	assertGoroutinesDrain(t, baseline)
}
