// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
)

// Scenario 1: another goroutine requests Close while OnOpen has not
// returned yet. The close request is forced to overlap OnOpen by a
// barrier; the connection must still produce exactly one OnOpen and one
// OnClose and its wait-group accounting must balance (Stop returns).
func TestLifecycle_CloseWhileOnOpenInFlight(t *testing.T) {
	closeStarted := make(chan struct{})
	letCloseFinish := make(chan struct{})

	g, addr, rec := newLifecycleEngine(t, 1, nil, lifecycleHooks{
		open: func(c *Conn) {
			go func() {
				close(closeStarted)
				_ = c.Close()
				close(letCloseFinish)
			}()
			// Park in OnOpen until the close request has actually
			// started, proving the request overlaps this callback.
			<-closeStarted
			<-letCloseFinish
		},
	})
	client := mustDial(t, addr)
	defer func() { _ = client.Close() }()

	svr := rec.waitOpen(t)

	// The peer must observe an orderly EOF (not a reset/leak).
	if _, err := client.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("peer read = %v, want EOF", err)
	}

	if err := rec.waitClose(t, svr); err != nil {
		t.Fatalf("OnClose error = %v, want nil (user Close)", err)
	}
	rec.assertOncePerConn(t)

	stopEngine(t, g)
	assertNoNbioGoroutines(t, g)
}

// Scenario 3a: from inside OnData an asynchronous write is requested and
// the connection is closed immediately afterwards on the poller
// goroutine. The late write must fail closed without re-publishing a
// close event.
func TestLifecycle_AsyncWriteThenCloseFromOnData(t *testing.T) {
	g, addr, rec := newLifecycleEngine(t, 1, nil, lifecycleHooks{
		data: func(c *Conn, _ []byte) {
			go func() { _, _ = c.Write([]byte("late")) }()
			_ = c.Close()
		},
	})
	client := mustDial(t, addr)
	defer func() { _ = client.Close() }()

	svr := rec.waitOpen(t)
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatalf("client write: %v", err)
	}

	if err := rec.waitClose(t, svr); err != nil {
		t.Fatalf("OnClose error = %v, want nil (user Close)", err)
	}
	rec.assertOncePerConn(t)

	stopEngine(t, g)
	assertNoNbioGoroutines(t, g)
}

// Scenario 3b: the asynchronous write is pinned so that Conn.Write is
// already past its closed check ordering relative to the close path,
// exercised on a multi-poller engine too.
func TestLifecycle_WriteStartRacesClose(t *testing.T) {
	startWrite := make(chan struct{})

	g, addr, rec := newLifecycleEngine(t, 2, nil, lifecycleHooks{
		data: func(c *Conn, _ []byte) {
			go func() {
				<-startWrite
				_, _ = c.Write([]byte("late"))
			}()
			close(startWrite)
			_ = c.Close()
		},
	})
	client := mustDial(t, addr)
	defer func() { _ = client.Close() }()

	svr := rec.waitOpen(t)
	if _, err := client.Write([]byte("y")); err != nil {
		t.Fatalf("client write: %v", err)
	}

	if err := rec.waitClose(t, svr); err != nil {
		t.Fatalf("OnClose error = %v, want nil (user Close)", err)
	}
	rec.assertOncePerConn(t)

	stopEngine(t, g)
	assertNoNbioGoroutines(t, g)
}

// Scenario 4: a new connection reaches addConn while Engine.Stop is
// running. addConn is parked until Stop has shut the listeners down and
// is draining in-flight registrations; Stop must still return promptly
// with no leaked connection or goroutine.
func TestLifecycle_AcceptDuringStop(t *testing.T) {
	parked := make(chan struct{})
	release := make(chan struct{})
	var parkedOnce int32

	prevBegin := testHookAddConnBegin
	prevStop := testHookStopListeners
	t.Cleanup(func() {
		testHookAddConnBegin = prevBegin
		testHookStopListeners = prevStop
	})
	testHookAddConnBegin = func(c *Conn) {
		if atomic.CompareAndSwapInt32(&parkedOnce, 0, 1) {
			close(parked)
			<-release
		}
	}

	g, addr, rec := newLifecycleEngine(t, 2, nil, lifecycleHooks{})

	client := mustDial(t, addr)
	defer func() { _ = client.Close() }()
	waitSignal(t, parked, "addConn parked during Stop")

	var stopped sync.WaitGroup
	stopped.Add(1)
	go func() {
		g.Stop()
		stopped.Done()
	}()
	close(release)

	stoppedCh := make(chan struct{})
	go func() {
		stopped.Wait()
		close(stoppedCh)
	}()
	select {
	case <-stoppedCh:
	case <-timerWatch(lifecycleTestTimeout):
		t.Fatalf("Engine.Stop deadlocked with an accept in flight")
	}

	// Every observed connection must balance exactly once.
	rec.assertOncePerConn(t)
	// The racing client must terminate instead of being served by a
	// half-stopped engine.
	_ = client.SetReadDeadline(deadlineSoon())
	if _, err := client.Read(make([]byte, 1)); err == nil {
		t.Fatalf("peer still served by a connection after Stop")
	}
	assertNoNbioGoroutines(t, g)
}

// Scenario 5: the user calls Close again from inside OnClose. Close must
// stay idempotent and OnClose must fire exactly once.
func TestLifecycle_ReentrantCloseFromOnClose(t *testing.T) {
	g, addr, rec := newLifecycleEngine(t, 1, nil, lifecycleHooks{
		close: func(c *Conn, err error) {
			_ = c.Close()
			_ = c.CloseWithError(io.EOF)
		},
	})
	client := mustDial(t, addr)

	svr := rec.waitOpen(t)
	if err := client.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}

	_ = rec.waitClose(t, svr)
	rec.assertOncePerConn(t)

	stopEngine(t, g)
	assertNoNbioGoroutines(t, g)
}
