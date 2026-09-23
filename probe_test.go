package nbio

import (
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func freeListenAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	return addr
}

// Probe: accept-during-Stop (strict interleave needs hooks; this one fires
// accepts while Stop is in flight naturally).
func TestProbeAcceptDuringStop(t *testing.T) {
	addr := freeListenAddr(t)
	var opens int64
	g := NewEngine(Config{Network: "tcp", Addrs: []string{addr}, NPoller: 2})
	g.OnOpen(func(c *Conn) { atomic.AddInt64(&opens, 1) })
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}

	stopDone := make(chan struct{})
	go func() {
		g.Stop()
		close(stopDone)
	}()

	// hammer accepts concurrently with stop
	deadline := time.Now().Add(500 * time.Millisecond)
	var conns []net.Conn
	for time.Now().Before(deadline) {
		c, err := net.Dial("tcp", addr)
		if err == nil {
			conns = append(conns, c)
		}
		runtime.Gosched()
	}
	select {
	case <-stopDone:
		t.Logf("Stop returned, opens=%d conns=%d", opens, len(conns))
	case <-time.After(10 * time.Second):
		t.Fatalf("Stop HUNG; opens=%d conns=%d", opens, len(conns))
	}
	for _, c := range conns {
		c.Close()
	}
}

func TestProbeAcceptPinned(t *testing.T) {
	addr := freeListenAddr(t)
	var opens int64
	var closes int64
	g := NewEngine(Config{Network: "tcp", Addrs: []string{addr}, NPoller: 1})
	g.OnOpen(func(c *Conn) { atomic.AddInt64(&opens, 1) })
	g.OnClose(func(c *Conn, err error) { atomic.AddInt64(&closes, 1) })
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}

	accepted := make(chan struct{})
	proceed := make(chan struct{})
	testAcceptAfterAddConnHook = func() {
		close(accepted)
		<-proceed
	}
	defer func() { testAcceptAfterAddConnHook = nil }()

	stopBegan := make(chan struct{})
	testStopAfterListenerStopHook = func() {
		close(stopBegan)
	}
	defer func() { testStopAfterListenerStopHook = nil }()

	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	<-accepted // addConn completed (onOpen fired, registered), acceptor stuck
	t.Logf("accepted, opens=%d", atomic.LoadInt64(&opens))

	stopDone := make(chan struct{})
	go func() {
		g.Stop()
		close(stopDone)
	}()
	<-stopBegan // listeners closed, about to snapshot conns
	// At this point addConn is complete but acceptor hasn't looped.
	// Release the acceptor.
	close(proceed)

	select {
	case <-stopDone:
		t.Logf("Stop OK opens=%d closes=%d", atomic.LoadInt64(&opens), atomic.LoadInt64(&closes))
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop HUNG opens=%d closes=%d", atomic.LoadInt64(&opens), atomic.LoadInt64(&closes))
	}
}

func TestProbeAcceptTorn(t *testing.T) {
	addr := freeListenAddr(t)
	var opens int64
	var closes int64
	g := NewEngine(Config{Network: "tcp", Addrs: []string{addr}, NPoller: 1})
	g.OnOpen(func(c *Conn) { atomic.AddInt64(&opens, 1) })
	g.OnClose(func(c *Conn, err error) { atomic.AddInt64(&closes, 1) })
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}

	accepted := make(chan struct{})
	proceed := make(chan struct{})
	testAcceptBeforeAddConnHook = func() {
		close(accepted)
		<-proceed
	}
	defer func() { testAcceptBeforeAddConnHook = nil }()

	snapshotDone := make(chan struct{})
	testStopAfterListenerStopHook = func() {
		// listener closed, conn accepted & parked before addConn; let snapshot proceed
		<-proceed
		close(snapshotDone)
	}
	defer func() { testStopAfterListenerStopHook = nil }()

	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	<-accepted

	stopDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Stop PANIC: %v", r)
			}
			close(stopDone)
		}()
		g.Stop()
	}()

	// Give Stop time to close listeners and reach the hook; then release
	// the acceptor so addConn races the conn snapshot.
	time.Sleep(50 * time.Millisecond)
	close(proceed)

	select {
	case <-stopDone:
		t.Logf("Stop returned opens=%d closes=%d (snapshot=%v)", atomic.LoadInt64(&opens), atomic.LoadInt64(&closes), snapshotDone != nil)
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop HUNG opens=%d closes=%d", atomic.LoadInt64(&opens), atomic.LoadInt64(&closes))
	}
}

func TestProbeAcceptTornStrict(t *testing.T) {
	addr := freeListenAddr(t)
	var opens int64
	var closes int64
	g := NewEngine(Config{Network: "tcp", Addrs: []string{addr}, NPoller: 1})
	g.OnOpen(func(c *Conn) { atomic.AddInt64(&opens, 1) })
	g.OnClose(func(c *Conn, err error) { atomic.AddInt64(&closes, 1) })
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}

	accepted := make(chan struct{})
	proceed := make(chan struct{})
	testAcceptBeforeAddConnHook = func() {
		close(accepted)
		<-proceed
	}
	defer func() { testAcceptBeforeAddConnHook = nil }()

	snapshotDone := make(chan struct{})
	testStopAfterSnapshotHook = func() {
		close(snapshotDone)
	}
	defer func() { testStopAfterSnapshotHook = nil }()

	client, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	<-accepted

	stopDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Stop PANIC: %v", r)
			}
			close(stopDone)
		}()
		g.Stop()
	}()

	select {
	case <-snapshotDone:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshot hook never fired")
	}
	// Now Stop has snapshotted conns without the new conn. Let addConn run.
	close(proceed)

	select {
	case <-stopDone:
		t.Logf("Stop returned opens=%d closes=%d", atomic.LoadInt64(&opens), atomic.LoadInt64(&closes))
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop HUNG opens=%d closes=%d", atomic.LoadInt64(&opens), atomic.LoadInt64(&closes))
	}
}
