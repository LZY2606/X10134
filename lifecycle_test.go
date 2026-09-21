// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// This file contains deterministic lifecycle tests. Each test pins a
// specific interleaving of Engine.Stop, connection close, queued writes
// and user callbacks using channels/barriers (and, where necessary, the
// unexported test hooks from testhook.go), then asserts that every
// callback fires exactly once, that the final error is of the expected
// class and that all goroutines can exit. Events of the same connection
// are kept in order; no global ordering between different connections
// is assumed.

// newLifecycleEngine starts an Engine listening on a loopback ephemeral
// port and registers a cleanup that stops it even if the test fails.
func newLifecycleEngine(t *testing.T, conf Config) *Engine {
	t.Helper()
	if conf.Network == "" {
		conf.Network = NETWORK_TCP
	}
	if len(conf.Addrs) == 0 {
		conf.Addrs = []string{"127.0.0.1:0"}
	}
	g := NewEngine(conf)
	if err := g.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	t.Cleanup(func() {
		stopLifecycleEngine(t, g)
	})
	return g
}

// stopLifecycleEngine stops the Engine and fails the test if Stop does
// not return in time, which catches Stop deadlocks and missed poller
// wakeups. It is safe to call more than once.
func stopLifecycleEngine(t *testing.T, g *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Engine.Stop did not return within 10s")
	}
}

// waitFor polls cond until it holds or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	if !waitCond(timeout, cond) {
		t.Fatalf("condition not met within %v: %v", timeout, msg)
	}
}

// waitCond is the non-failing variant of waitFor, usable from helper
// goroutines that may not call t.Fatalf.
func waitCond(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return cond()
		}
		time.Sleep(time.Millisecond * 5)
	}
}

// recvCloseErr waits for one error reported on an OnClose channel.
func recvCloseErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %v", what)
		return nil
	}
}

// assertGoroutinesSettled polls until the goroutine count returns to the
// baseline captured before the engine under test was started.
func assertGoroutinesSettled(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		n := runtime.NumGoroutine()
		if n <= baseline {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("goroutines did not settle: baseline=%v, now=%v", baseline, n)
		}
		time.Sleep(time.Millisecond * 10)
	}
}

// TestLifecycleOnOpenConcurrentClose covers the interleaving where
// another goroutine requests Close while the user's OnOpen callback has
// not returned yet. The close must win exactly once: OnClose fires with
// a nil error while OnOpen is still blocked, and the connection must not
// be registered for io events afterwards.
func TestLifecycleOnOpenConcurrentClose(t *testing.T) {
	baseline := runtime.NumGoroutine()
	g := newLifecycleEngine(t, Config{})

	entered := make(chan *Conn, 1)
	release := make(chan struct{})
	var openCount, closeCount int32
	g.OnOpen(func(c *Conn) {
		atomic.AddInt32(&openCount, 1)
		entered <- c
		<-release
	})
	closeErrs := make(chan error, 4)
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closeCount, 1)
		closeErrs <- err
	})

	client, err := net.Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// OnOpen has been entered and is blocked on the barrier.
	var c *Conn
	select {
	case c = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("OnOpen not entered")
	}

	// Request close from another goroutine while OnOpen is blocked.
	closeReturned := make(chan struct{})
	go func() {
		_ = c.Close()
		close(closeReturned)
	}()

	// OnClose must fire exactly once, with nil error, even though OnOpen
	// has not returned yet.
	if err := recvCloseErr(t, closeErrs, "OnClose while OnOpen blocked"); err != nil {
		t.Fatalf("OnClose err = %v, want nil", err)
	}
	<-closeReturned

	// Let OnOpen return; the connection must not come back to life.
	close(release)

	// A second Close is a no-op and must not re-trigger OnClose.
	_ = c.Close()

	stopLifecycleEngine(t, g)

	if n := atomic.LoadInt32(&openCount); n != 1 {
		t.Fatalf("OnOpen count = %v, want 1", n)
	}
	if n := atomic.LoadInt32(&closeCount); n != 1 {
		t.Fatalf("OnClose count = %v, want 1", n)
	}
	if closed, _ := c.IsClosed(); !closed {
		t.Fatal("connection should be closed")
	}
	assertGoroutinesSettled(t, baseline)
}

// TestLifecycleStopDuringAccept covers the interleaving where a new
// connection completes accept (it is inside OnOpen) while Engine.Stop is
// already draining. Stop must not hang, the connection must be closed
// exactly once, and no goroutine may be left behind.
func TestLifecycleStopDuringAccept(t *testing.T) {
	baseline := runtime.NumGoroutine()
	g := newLifecycleEngine(t, Config{})

	entered := make(chan *Conn, 1)
	release := make(chan struct{})
	var openCount, closeCount int32
	g.OnOpen(func(c *Conn) {
		atomic.AddInt32(&openCount, 1)
		entered <- c
		<-release
	})
	closeErrs := make(chan error, 4)
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closeCount, 1)
		closeErrs <- err
	})

	client, err := net.Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	// The accept has completed and OnOpen is blocked on the barrier.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("OnOpen not entered")
	}

	// Pin Stop right after it has marked the engine as shutting down and
	// has taken the connection snapshot: the accepted connection is not
	// in the snapshot and is still blocked in OnOpen.
	stopDraining := make(chan struct{})
	testHookBeforeShutdownWait = func() { close(stopDraining) }
	defer func() { testHookBeforeShutdownWait = nil }()

	stopDone := make(chan struct{})
	go func() {
		g.Stop()
		close(stopDone)
	}()

	select {
	case <-stopDraining:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not reach the drain phase")
	}

	// Let the in-flight accept finish while Stop is waiting for
	// connections to close.
	close(release)

	select {
	case <-stopDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop hung while an accept was in flight")
	}

	if n := atomic.LoadInt32(&openCount); n != 1 {
		t.Fatalf("OnOpen count = %v, want 1", n)
	}
	if n := atomic.LoadInt32(&closeCount); n != 1 {
		t.Fatalf("OnClose count = %v, want 1", n)
	}
	// The connection is closed either by the shutdown path of addConn
	// (errEngineStopped) or by Stop's own connection snapshot (nil).
	err = recvCloseErr(t, closeErrs, "OnClose for the in-flight accept")
	if err != nil && !errors.Is(err, errEngineStopped) {
		t.Fatalf("OnClose err = %v, want nil or errEngineStopped", err)
	}
	assertGoroutinesSettled(t, baseline)
}

// TestLifecycleCloseInsideOnClose covers the interleaving where the
// user's OnClose callback closes the connection again. The nested Close
// must be a no-op: no deadlock, no second OnClose, and the first close
// error must be preserved.
func TestLifecycleCloseInsideOnClose(t *testing.T) {
	baseline := runtime.NumGoroutine()
	g := newLifecycleEngine(t, Config{})

	opened := make(chan *Conn, 1)
	g.OnOpen(func(c *Conn) {
		opened <- c
	})

	errCustom := errors.New("custom close error")
	var closeCount int32
	closeErrs := make(chan error, 4)
	onCloseReturned := make(chan struct{})
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closeCount, 1)
		closeErrs <- err
		// Closing again from inside OnClose must be a no-op and must
		// neither deadlock nor re-enter OnClose.
		_ = c.Close()
		_ = c.CloseWithError(errors.New("error that must be dropped"))
		close(onCloseReturned)
	})

	client, err := net.Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatalf("Dial failed: %v", err)
	}
	defer func() { _ = client.Close() }()

	var c *Conn
	select {
	case c = <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("OnOpen not entered")
	}

	if err := c.CloseWithError(errCustom); err != nil {
		t.Fatalf("CloseWithError failed: %v", err)
	}

	// The first close error must be delivered to OnClose.
	if err := recvCloseErr(t, closeErrs, "OnClose"); !errors.Is(err, errCustom) {
		t.Fatalf("OnClose err = %v, want %v", err, errCustom)
	}
	select {
	case <-onCloseReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("OnClose did not return: nested Close deadlocked")
	}

	// Yet another close from a different goroutine is a no-op too.
	_ = c.Close()

	stopLifecycleEngine(t, g)

	if n := atomic.LoadInt32(&closeCount); n != 1 {
		t.Fatalf("OnClose count = %v, want 1", n)
	}
	// The first close error wins and is preserved.
	if closed, cerr := c.IsClosed(); !closed || !errors.Is(cerr, errCustom) {
		t.Fatalf("IsClosed = (%v, %v), want (true, %v)", closed, cerr, errCustom)
	}
	assertGoroutinesSettled(t, baseline)
}
