// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"net"
	"testing"
	"time"
)

// resetTestHooks clears the package level test hooks even when a test fails
// early, so one failing interleaving cannot wedge the next test.
func resetTestHooks() {
	testHookAcceptConn = nil
	testHookStopSnapshot = nil
	testHookWriteEnter = nil
	testHookWriteLocked = nil
	resetTestHooksPlatform()
}

// TestLifecycleCloseDuringOnOpen pins an accepted connection inside its
// OnOpen callback and requests Close from another goroutine before OnOpen
// returns. The close must complete exactly once, the write path must report
// ErrClosed afterwards, and engine shutdown must not hang.
func TestLifecycleCloseDuringOnOpen(t *testing.T) {
	baseline := pollerBaseline()
	st := newLifecycleStats()

	inOpen := newGate()
	releaseOpen := newGate()
	gotConn := make(chan *Conn, 1)

	g := startEngine(t, lifecycleEngineHooks(t, st, nil, func(c *Conn) {
		gotConn <- c
		inOpen.signal()
		releaseOpen.hold()
	}, nil))
	defer func() {
		stopEngine(t, g)
		noLeakedGoroutines(t, baseline)
	}()

	client := dialRaw(t, g.Addrs[0])
	defer client.Close()

	c := <-gotConn
	inOpen.waitArrived(t, "OnOpen")

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()

	// The concurrent close must mark the conn closed while OnOpen is still
	// in progress.
	select {
	case err := <-closeDone:
		_ = err
	case <-time.After(time.Second * 5):
		t.Fatalf("Close blocked while OnOpen was in progress")
	}

	releaseOpen.letGo()

	cc := st.waitClose(t, "close during OnOpen")
	if cc.c != c {
		t.Fatalf("OnClose fired for unexpected conn")
	}
	if st.closesOf(c) != 1 {
		t.Fatalf("OnClose count = %v, want 1", st.closesOf(c))
	}

	// A write after close must fail with ErrClosed, never panic.
	if _, err := c.Write([]byte("late")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("post-close Write err = %v, want ErrClosed", err)
	}

	// A second close is idempotent.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close returned %v, want nil", err)
	}
	st.sameConnOrdered(t)
}

// TestLifecycleWriteDuringOnDataClose covers the interleaving where OnData
// triggers a write from a separate goroutine and the connection is closed
// immediately, with the writer pinned at fixed points of its code path.
func TestLifecycleWriteDuringOnDataClose(t *testing.T) {
	t.Run("write_then_close", testWriteThenCloseDuringOnData)
	t.Run("close_then_write", testCloseThenWriteDuringOnData)
}

func testWriteThenCloseDuringOnData(t *testing.T) {
	defer resetTestHooks()
	baseline := pollerBaseline()
	st := newLifecycleStats()

	gotConn := make(chan *Conn, 1)
	writerLocked := make(chan struct{})
	releaseWriter := make(chan struct{})
	writerStarted := make(chan *Conn, 1)
	writeReturned := make(chan error, 1)

	g := startEngine(t, lifecycleEngineHooks(t, st, nil,
		func(c *Conn) { gotConn <- c },
		func(c *Conn, data []byte) {
			// The write is triggered from OnData onto another goroutine,
			// exactly the interleaving under test.
			go func() {
				_, err := c.Write(make([]byte, 64*1024))
				writeReturned <- err
			}()
			writerStarted <- c
		}))
	testHookWriteLocked = func() {
		select {
		case <-writerLocked:
		default:
			close(writerLocked)
		}
		<-releaseWriter
	}
	defer func() {
		stopEngine(t, g)
		noLeakedGoroutines(t, baseline)
	}()

	client := dialRaw(t, g.Addrs[0])
	defer client.Close()
	c := <-gotConn
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	<-writerStarted
	<-writerLocked

	// Close while the writer holds the conn mutex; it parks until the write
	// completes, then runs the single close transition.
	closeReturned := make(chan error, 1)
	go func() { closeReturned <- c.Close() }()
	close(releaseWriter)
	select {
	case <-writeReturned:
	case <-time.After(time.Second * 5):
		t.Fatalf("Write did not return after interleaving")
	}
	select {
	case <-closeReturned:
	case <-time.After(time.Second * 5):
		t.Fatalf("Close did not finish after writer released the mutex")
	}

	cc := st.waitClose(t, "write then close")
	if cc.c != c {
		t.Fatalf("OnClose fired for unexpected conn")
	}
	if st.closesOf(c) != 1 {
		t.Fatalf("OnClose count = %v, want 1", st.closesOf(c))
	}
	st.sameConnOrdered(t)
}

func testCloseThenWriteDuringOnData(t *testing.T) {
	defer resetTestHooks()
	baseline := pollerBaseline()
	st := newLifecycleStats()

	gotConn := make(chan *Conn, 1)
	writerAtEnter := make(chan struct{})
	releaseEnter := make(chan struct{})
	writerStarted := make(chan struct{})
	writeReturned := make(chan error, 1)

	g := startEngine(t, lifecycleEngineHooks(t, st, nil,
		func(c *Conn) { gotConn <- c },
		func(c *Conn, data []byte) {
			go func() {
				_, err := c.Write(make([]byte, 64))
				writeReturned <- err
			}()
			close(writerStarted)
		}))
	testHookWriteEnter = func() {
		select {
		case <-writerAtEnter:
		default:
			close(writerAtEnter)
		}
		<-releaseEnter
	}
	defer func() {
		stopEngine(t, g)
		noLeakedGoroutines(t, baseline)
	}()

	client := dialRaw(t, g.Addrs[0])
	defer client.Close()
	c := <-gotConn
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatalf("client write: %v", err)
	}
	<-writerStarted
	<-writerAtEnter

	// Close wins while the writer is parked before taking the conn mutex.
	if err := c.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}
	close(releaseEnter)
	select {
	case err := <-writeReturned:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Write after close err = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second * 5):
		t.Fatalf("Write did not return after close")
	}

	cc := st.waitClose(t, "close then write")
	if cc.c != c {
		t.Fatalf("OnClose fired for unexpected conn")
	}
	if st.closesOf(c) != 1 {
		t.Fatalf("OnClose count = %v, want 1", st.closesOf(c))
	}
	st.sameConnOrdered(t)
}

// TestLifecycleAcceptDuringStop arranges for a new connection to finish its
// accept at the precise moment Engine.Stop is closing the listeners and
// snapshotting the connection tables. The accepted conn is either fully
// tracked and closed by Stop, or rejected; Stop must always return.
func TestLifecycleAcceptDuringStop(t *testing.T) {
	defer resetTestHooks()
	baseline := pollerBaseline()
	st := newLifecycleStats()

	accepted := newGate()
	releaseAccept := newGate()
	stopSnapshot := newGate()
	releaseSnapshot := newGate()

	g := lifecycleEngineConfigured(t, st, nil, nil, nil, func(g *Engine) {
		testHookAcceptConn = func() {
			accepted.signal()
			releaseAccept.hold()
		}
		testHookStopSnapshot = func() {
			stopSnapshot.signal()
			releaseSnapshot.hold()
		}
	})

	client := dialRaw(t, g.Addrs[0])
	defer client.Close()

	accepted.waitArrived(t, "connection accepted")

	stopReturned := make(chan struct{})
	go func() {
		g.Stop()
		close(stopReturned)
	}()
	stopSnapshot.waitArrived(t, "Stop snapshot")

	// Release both barriers: addConn races the snapshot under g.mux.
	releaseAccept.letGo()
	releaseSnapshot.letGo()

	select {
	case <-stopReturned:
	case <-time.After(time.Second * 10):
		t.Fatalf("Engine.Stop hung with in-flight accept")
	}
	noLeakedGoroutines(t, baseline)

	// If the connection was published it must be closed by Stop with
	// exactly one OnClose; otherwise OnOpen must never have run.
	select {
	case cc := <-st.closeCh:
		if st.closesOf(cc.c) != 1 {
			t.Fatalf("OnClose count = %v, want 1", st.closesOf(cc.c))
		}
		st.sameConnOrdered(t)
	default:
		select {
		case c := <-st.openCh:
			t.Fatalf("rejected conn got late OnOpen: %v", c)
		default:
		}
	}
}

// TestLifecycleCloseInsideOnClose verifies that calling Close again from the
// user OnClose callback is idempotent: no recursion, no duplicate callback,
// and the first closing error is preserved.
func TestLifecycleCloseInsideOnClose(t *testing.T) {
	baseline := pollerBaseline()
	st := newLifecycleStats()

	gotConn := make(chan *Conn, 1)
	userClose := newGate()

	g := startEngine(t, lifecycleEngineHooks(t, st, nil,
		func(c *Conn) { gotConn <- c },
		nil))
	g.OnClose(func(c *Conn, err error) {
		st.onClose(c, err)
		// Re-entrant close must be a no-op.
		if cerr := c.Close(); cerr != nil {
			t.Errorf("Close inside OnClose returned %v", cerr)
		}
		userClose.signal()
	})
	defer func() {
		stopEngine(t, g)
		noLeakedGoroutines(t, baseline)
	}()

	client := dialRaw(t, g.Addrs[0])
	defer client.Close()
	c := <-gotConn

	if err := c.CloseWithError(errReadTimeout); err != nil {
		t.Fatalf("CloseWithError returned %v", err)
	}

	userClose.waitArrived(t, "OnClose with re-entrant Close")
	if st.closesOf(c) != 1 {
		t.Fatalf("OnClose count = %v, want 1", st.closesOf(c))
	}
	if _, isClosed := c.IsClosed(); !errors.Is(isClosed, ErrReadTimeout) {
		t.Fatalf("close err = %v, want %v", isClosed, ErrReadTimeout)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("final Close returned %v, want nil", err)
	}
	st.sameConnOrdered(t)
}
