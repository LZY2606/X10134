// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio_test

// Deterministic lifecycle tests for Engine/Conn shutdown ordering.
//
// Every race below is narrowed to a single interleaving with channels and
// barriers; no random stress or sleeps are used for synchronization.

import (
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lesismal/nbio"
)

const lifecycleWait = 5 * time.Second

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleWait):
		t.Fatalf("timeout waiting for %s", what)
	}
}

// gateListener blocks Accept until release is closed.
type gateListener struct {
	net.Listener
	release       chan struct{}
	once          sync.Once
	stalled       chan struct{}
	closeObserved chan struct{}
	closeOnce     sync.Once
}

func newGateListener(t *testing.T) (string, *gateListener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return ln.Addr().String(), &gateListener{
		Listener:      ln,
		release:       make(chan struct{}),
		stalled:       make(chan struct{}, 1),
		closeObserved: make(chan struct{}),
	}
}

// Close is called by Engine.Stop; it records that shutdown reached the
// listener stage so the test can release the parked Accept at exactly that
// point in time.
func (l *gateListener) Close() error {
	l.closeOnce.Do(func() { close(l.closeObserved) })
	return l.Listener.Close()
}

func (l *gateListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return conn, err
	}
	select {
	case l.stalled <- struct{}{}:
	default:
	}
	<-l.release
	return conn, nil
}

func (l *gateListener) letGo() {
	l.once.Do(func() { close(l.release) })
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}

// assertGoroutinesDrain compares the goroutine count against the pre-engine
// baseline (the package's global engine in nbio_test.go lives until TestStop).
func assertGoroutinesDrain(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+2 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Fatalf("goroutines leaked: before=%d after=%d\n%s", baseline, runtime.NumGoroutine(), buf[:n])
}

func startTestEngine(t *testing.T, conf nbio.Config) *nbio.Engine {
	t.Helper()
	g := nbio.NewEngine(conf)
	if err := g.Start(); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	return g
}

// startListeningEngine starts an engine that owns a listener on an
// ephemeral loopback port and returns the engine and dial address.
func startListeningEngine(t *testing.T, conf nbio.Config) (*nbio.Engine, string) {
	t.Helper()
	conf.Network = "tcp"
	conf.Addrs = []string{"127.0.0.1:0"}
	g := startTestEngine(t, conf)
	return g, g.Addrs[0]
}

// startEphemeral starts an already-configured engine listening on a random
// loopback port and returns the dial address.
func startEphemeral(t *testing.T, g *nbio.Engine) string {
	t.Helper()
	g.Network = "tcp"
	g.Addrs = []string{"127.0.0.1:0"}
	if err := g.Start(); err != nil {
		t.Fatalf("start engine: %v", err)
	}
	return g.Addrs[0]
}

func stopEngineAsync(t *testing.T, g *nbio.Engine) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	return done
}
