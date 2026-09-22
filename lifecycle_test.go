// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Scenario S1: another goroutine requests Close while the OnOpen
// callback has not returned yet.
//
// The interleaving is pinned with channels:
//  1. OnOpen runs and blocks until the test allows it to return.
//  2. While blocked, a separate goroutine calls Close.
//  3. OnClose fires exactly once, and Stop never deadlocks on wgConn.
func TestLifecycleCloseDuringOnOpen(t *testing.T) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	enterOpen := make(chan struct{})
	allowReturn := make(chan struct{})
	serverClosed := make(chan struct{})
	var connHolder connHolder

	counters := newConnCounters()
	g, _ := newListeningEngine(t, Config{Name: "lc-open", NPoller: 2}, func(g *Engine) {
		g.OnOpen(func(c *Conn) {
			counters.markOpen(c)
			connHolder.set(c)
			close(enterOpen)
			<-allowReturn
		})
		g.OnData(func(c *Conn, data []byte) {})
		g.OnClose(func(c *Conn, err error) {
			counters.markClose(c)
			close(serverClosed)
		})
	})

	peer := dialRaw(t, g.Addrs[0])
	defer func() { _ = peer.Close() }()

	waitSignal(t, enterOpen, "OnOpen entry")

	// Concurrent Close from a different goroutine while OnOpen runs.
	go func() {
		_ = connHolder.get().Close()
	}()

	// Let the Close goroutine genuinely race the still-parked OnOpen;
	// either linearization is valid behavior, not a scheduling contract.
	time.Sleep(50 * time.Millisecond)
	close(allowReturn)

	waitSignal(t, serverClosed, "OnClose")
	counters.assertOnceEach(t)

	// Peer must observe the server side has gone away.
	_ = peer.SetReadDeadline(time.Now().Add(lifecycleTestTimeout))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatalf("peer expected EOF after server Close, got data")
	}

	// Second Close is harmless: no duplicated event.
	c := connHolder.get()
	if err := c.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := counters.closeCount(c); n != 1 {
		t.Fatalf("OnClose fired %d times, want 1", n)
	}
}

// connHolder publishes the accepted *Conn from the OnOpen goroutine.
type connHolder struct {
	mu sync.Mutex
	c  *Conn
}

func (h *connHolder) set(c *Conn) {
	h.mu.Lock()
	if h.c == nil {
		h.c = c
	}
	h.mu.Unlock()
}

func (h *connHolder) get() *Conn {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.c
}

// Scenario S3a: OnData triggers an inline write then closes immediately
// within the same callback.
func TestLifecycleWriteThenCloseInOnData(t *testing.T) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	alloc := newTrackingAllocator(t)
	serverClosed := make(chan struct{})

	counters := newConnCounters()
	g, _ := newListeningEngine(t, Config{Name: "lc-wc-a", NPoller: 1, BodyAllocator: alloc}, func(g *Engine) {
		g.OnOpen(func(c *Conn) { counters.markOpen(c) })
		g.OnData(func(c *Conn, data []byte) {
			if _, err := c.Write(data); err != nil {
				t.Errorf("write in OnData failed: %v", err)
				return
			}
			_ = c.Close()
		})
		g.OnClose(func(c *Conn, err error) {
			counters.markClose(c)
			close(serverClosed)
		})
	})

	peer := dialRaw(t, g.Addrs[0])
	payload := []byte("hello-inline")
	if _, err := peer.Write(payload); err != nil {
		t.Fatal(err)
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(peer, got); err != nil {
		t.Fatalf("peer read echo: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload mismatch: %q", got)
	}
	_ = peer.SetReadDeadline(time.Now().Add(lifecycleTestTimeout))
	if _, err := peer.Read(make([]byte, 1)); err == nil {
		t.Fatal("peer expected EOF")
	}

	waitSignal(t, serverClosed, "OnClose")
	counters.assertOnceEach(t)
	alloc.assertBalanced(t)
}

// Scenario S3b: OnData starts an async write from another goroutine and
// closes immediately, covering both fixed linearizations.
func TestLifecycleAsyncWriteRacesClose(t *testing.T) {
	for _, writeFirst := range []bool{false, true} {
		name := "closeFirst"
		if writeFirst {
			name = "writeFirst"
		}
		t.Run(name, func(t *testing.T) {
			testAsyncWriteRacesClose(t, writeFirst)
		})
	}
}

func testAsyncWriteRacesClose(t *testing.T, writeFirst bool) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	alloc := newTrackingAllocator(t)
	serverClosed := make(chan struct{})
	// The two goroutines are serialized by a rendezvous channel so the
	// linearization is fixed, not scheduler dependent.
	rendezvous := make(chan struct{})
	writeResult := make(chan error, 1)

	counters := newConnCounters()
	g, _ := newListeningEngine(t, Config{Name: "lc-wc-b", NPoller: 2, BodyAllocator: alloc}, func(g *Engine) {
		g.OnOpen(func(c *Conn) { counters.markOpen(c) })
		g.OnData(func(c *Conn, data []byte) {
			payload := append([]byte{}, data...)
			if writeFirst {
				// 1) writer waits for permission, runs Write, signals;
				// 2) closer then runs Close.
				go func() {
					<-rendezvous
					_, err := c.Write(payload)
					writeResult <- err
				}()
				close(rendezvous)
				if err := <-writeResult; err != nil {
					t.Errorf("pinned writeFirst Write: %v", err)
				}
				_ = c.Close()
			} else {
				// 1) closer waits for permission, runs Close, signals;
				// 2) writer then runs Write and must get net.ErrClosed.
				go func() {
					<-rendezvous
					_ = c.Close()
					writeResult <- nil
				}()
				close(rendezvous)
				<-writeResult
				if _, err := c.Write(payload); !isLocalClosed(err) {
					t.Errorf("pinned closeFirst Write err = %v, want net.ErrClosed", err)
				}
			}
		})
		g.OnClose(func(c *Conn, err error) {
			counters.markClose(c)
			close(serverClosed)
		})
	})

	peer := dialRaw(t, g.Addrs[0])
	payload := []byte("async-write-data")
	if _, err := peer.Write(payload); err != nil {
		t.Fatal(err)
	}

	waitSignal(t, serverClosed, "OnClose")

	if writeFirst {
		got := make([]byte, len(payload))
		_ = peer.SetReadDeadline(time.Now().Add(lifecycleTestTimeout))
		if _, err := io.ReadFull(peer, got); err != nil {
			t.Fatalf("peer read echo in writeFirst: %v", err)
		}
		if string(got) != string(payload) {
			t.Fatalf("payload mismatch: %q", got)
		}
	}

	_ = peer.SetReadDeadline(time.Now().Add(lifecycleTestTimeout))
	_ = peer.Close()

	counters.assertOnceEach(t)
	alloc.assertBalanced(t)
}

// Scenario S4a: accepted raw conn is held between Accept and addConn
// while Stop runs; registration is rejected, no callbacks fire, Stop
// returns.
func TestLifecycleAcceptWhileStoppingReject(t *testing.T) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	accepted := make(chan struct{})
	allowRegister := make(chan struct{})
	snapshotDone := make(chan struct{})

	counters := newConnCounters()
	conf := Config{Name: "lc-s4a", NPoller: 2}
	g, stop := newListeningEngine(t, conf, func(g *Engine) {
		g.OnOpen(func(c *Conn) { counters.markOpen(c) })
		g.OnClose(func(c *Conn, err error) { counters.markClose(c) })
	})

	hookAfterAccept = func(raw interface{}) {
		close(accepted)
		<-allowRegister
	}
	hookStopAfterConns = func() { close(snapshotDone) }

	peerCh := make(chan net.Conn, 1)
	go func() {
		c, err := net.Dial("tcp", g.Addrs[0])
		if err == nil {
			peerCh <- c
			return
		}
		peerCh <- nil
	}()

	waitSignal(t, accepted, "acceptor holding raw conn")

	stopDone := make(chan struct{})
	go func() {
		stop()
		close(stopDone)
	}()
	waitSignal(t, snapshotDone, "stop snapshot")
	close(allowRegister)

	select {
	case <-stopDone:
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("Stop deadlocked with accept parked between Accept and addConn")
	}

	if n := counters.totalOpens(); n != 0 {
		t.Fatalf("OnOpen fired %d times for rejected conn, want 0", n)
	}
	if n := counters.totalCloses(); n != 0 {
		t.Fatalf("OnClose fired %d times for rejected conn, want 0", n)
	}

	if c := <-peerCh; c != nil {
		_ = c.SetReadDeadline(time.Now().Add(lifecycleTestTimeout))
		if _, err := c.Read(make([]byte, 1)); err == nil {
			t.Fatal("peer expected EOF for rejected conn")
		}
		_ = c.Close()
	}
}

// Scenario S4b: a conn is registered before Stop snapshots conns; Stop
// closes it exactly once and wakes every io poller.
func TestLifecycleAcceptJustBeforeStop(t *testing.T) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	opened := make(chan struct{})
	serverClosed := make(chan struct{})
	var holder connHolder

	counters := newConnCounters()
	conf := Config{Name: "lc-s4b", NPoller: 3}
	g, stop := newListeningEngine(t, conf, func(g *Engine) {
		g.OnOpen(func(c *Conn) {
			holder.set(c)
			counters.markOpen(c)
			close(opened)
		})
		g.OnClose(func(c *Conn, err error) {
			counters.markClose(c)
			close(serverClosed)
		})
	})

	// Park the *next* accept while Stop is in flight.
	secondAccepted := make(chan struct{})
	allowSecond := make(chan struct{})
	var acceptCount int32
	hookAfterAccept = func(raw interface{}) {
		if atomic.AddInt32(&acceptCount, 1) == 2 {
			close(secondAccepted)
			<-allowSecond
		}
	}

	pollerStops := make(chan int, 16)
	var stopMu sync.Mutex
	stopped := map[int]int{}
	hookPollerStop = func(index int, isListener bool) {
		if !isListener {
			stopMu.Lock()
			stopped[index]++
			stopMu.Unlock()
			pollerStops <- index
		}
	}

	peer := dialRaw(t, g.Addrs[0])
	defer func() { _ = peer.Close() }()
	waitSignal(t, opened, "first conn OnOpen")

	peer2Ch := make(chan net.Conn, 1)
	go func() {
		c, err := net.Dial("tcp", g.Addrs[0])
		if err == nil {
			peer2Ch <- c
			return
		}
		peer2Ch <- nil
	}()
	waitSignal(t, secondAccepted, "second accept parked")

	stopDone := make(chan struct{})
	go func() {
		stop()
		close(stopDone)
	}()

	waitSignal(t, serverClosed, "registered conn OnClose")
	close(allowSecond)

	select {
	case <-stopDone:
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("Stop deadlocked while a conn is registered")
	}

	if n := counters.closeCount(holder.get()); n != 1 {
		t.Fatalf("registered conn OnClose %d times, want 1", n)
	}
	counters.assertOnceEach(t)

	want := g.NPoller
	for i := 0; i < want; i++ {
		select {
		case <-pollerStops:
		case <-time.After(lifecycleTestTimeout):
			t.Fatalf("only %d/%d pollers stopped", i, want)
		}
	}
	stopMu.Lock()
	for idx, n := range stopped {
		if n != 1 {
			t.Fatalf("poller %d stopped %d times, want 1", idx, n)
		}
	}
	stopMu.Unlock()

	if c2 := <-peer2Ch; c2 != nil {
		_ = c2.SetReadDeadline(time.Now().Add(lifecycleTestTimeout))
		_, _ = c2.Read(make([]byte, 1))
		_ = c2.Close()
	}
}

// Scenario S5: user OnClose handler calls Close again.
func TestLifecycleReCloseInsideOnClose(t *testing.T) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	serverClosed := make(chan struct{})

	counters := newConnCounters()
	g, _ := newListeningEngine(t, Config{Name: "lc-s5", NPoller: 1}, func(g *Engine) {
		g.OnOpen(func(c *Conn) { counters.markOpen(c) })
		g.OnData(func(c *Conn, data []byte) {
			_, _ = c.Write(data)
		})
		g.OnClose(func(c *Conn, err error) {
			// Re-entrant Close must not recurse or re-publish.
			if cerr := c.Close(); cerr != nil {
				t.Errorf("Close inside OnClose returned error: %v", cerr)
			}
			counters.markClose(c)
			close(serverClosed)
		})
	})

	peer := dialRaw(t, g.Addrs[0])
	if _, err := peer.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = peer.SetReadDeadline(time.Now().Add(lifecycleTestTimeout))
	if _, err := io.ReadFull(peer, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()

	waitSignal(t, serverClosed, "OnClose")
	time.Sleep(50 * time.Millisecond)
	counters.assertOnceEach(t)
}
