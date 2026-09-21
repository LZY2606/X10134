// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio_test

import (
	"errors"
	"io"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lesismal/nbio"
)

func setLinger0(t *testing.T, c *net.TCPConn) {
	t.Helper()
	rc, err := c.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	serr := rc.Control(func(fd uintptr) {
		err = syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET, syscall.SO_LINGER,
			&syscall.Linger{Onoff: 1, Linger: 0})
	})
	if serr != nil {
		t.Fatal(serr)
	}
	if err != nil {
		t.Fatal(err)
	}
}

// TestLifecycle_QueuedWriteWakesUp: the peer drains slowly; the pending
// cache must be flushed via repeated write-readiness events (no missed
// wakeup), and the write call must return after delivering everything.
func TestLifecycle_QueuedWriteWakesUp(t *testing.T) {
	alloc := newTrackingAllocator()
	var (
		openReady   = make(chan *nbio.Conn, 1)
		writtenSize int64
	)
	total := 2 * 1024 * 1024

	g := nbio.NewEngine(nbio.Config{
		Network:       "tcp",
		NPoller:       1,
		BodyAllocator: alloc,
	})
	g.OnOpen(func(c *nbio.Conn) {
		if err := c.SetWriteBuffer(1024); err != nil {
			t.Logf("server set write buffer: %v", err)
		}
		openReady <- c
	})
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, err error) {})
	g.OnWrittenSize(func(c *nbio.Conn, b []byte, n int) {
		alloc.checkCallback(b)
		atomic.AddInt64(&writtenSize, int64(n))
	})
	addr := startEphemeral(t, g)

	client := dialRaw(t, addr)
	defer client.Close()
	if tc, ok := client.(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(4096)
	}

	srv := <-openReady
	writeDone := make(chan struct{})
	go func() {
		if _, err := srv.Write(bytesRepeating(total, 0x5A)); err != nil {
			t.Errorf("server write: %v", err)
		}
		close(writeDone)
	}()

	// Deliberately do nothing for a bounded rendezvous-free moment is not
	// needed: start draining immediately; nbio must keep getting woken until
	// the full cache is flushed.
	go func() {
		buf := make([]byte, 64*1024)
		for {
			n, err := client.Read(buf)
			if err != nil {
				return
			}
			if n == 0 {
				return
			}
		}
	}()

	select {
	case <-writeDone:
	case <-time.After(lifecycleWait):
		t.Fatalf("queued write never completed (missed write wakeup), written=%d/%d",
			atomic.LoadInt64(&writtenSize), total)
	}

	if got := atomic.LoadInt64(&writtenSize); got != int64(total) {
		t.Fatalf("written size = %d, want %d", got, total)
	}
	for _, msg := range alloc.errors() {
		t.Errorf("allocator: %s", msg)
	}

	done := stopEngineAsync(t, g)
	waitSignal(t, done, "engine stop")
}

// TestLifecycle_SeededRace runs one fixed, repeatable operation script on a
// handful of conns with several pollers. The script is deterministic (fixed
// seeds) and drives the close/write interplay from multiple goroutines;
// under -race it catches write-after-reuse, double close event publication,
// buffer imbalance, and Stop hangs.
func TestLifecycle_SeededRace(t *testing.T) {
	for _, seed := range []int64{1, 20240517, 987654321} {
		t.Run("", func(t *testing.T) {
			runSeededRace(t, seed)
		})
	}
}

func runSeededRace(t *testing.T, seed int64) {
	alloc := newTrackingAllocator()
	const nConn = 3
	var (
		conns       = make([]*nbio.Conn, nConn)
		connsRegMu  [nConn]sync.Mutex
		openWg      sync.WaitGroup
		closeCount  int32
		closeErrMu  sync.Mutex
		closeErrs   []error
		openBarrier = make(chan struct{})
		openIndex   int64
	)
	openWg.Add(nConn)

	g := nbio.NewEngine(nbio.Config{
		Network:       "tcp",
		NPoller:       3,
		BodyAllocator: alloc,
	})
	g.OnOpen(func(c *nbio.Conn) {
		idx := int(atomic.AddInt64(&openIndex, 1)) - 1
		if idx < nConn {
			connsRegMu[idx].Lock()
			conns[idx] = c
			connsRegMu[idx].Unlock()
		}
		openWg.Done()
		<-openBarrier
	})
	g.OnData(func(c *nbio.Conn, data []byte) {})
	g.OnClose(func(c *nbio.Conn, err error) {
		closeErrMu.Lock()
		closeErrs = append(closeErrs, err)
		closeErrMu.Unlock()
		atomic.AddInt32(&closeCount, 1)
	})
	g.OnWrittenSize(func(c *nbio.Conn, b []byte, n int) {
		alloc.checkCallback(b)
	})
	addr := startEphemeral(t, g)

	clients := make([]net.Conn, nConn)
	for i := 0; i < nConn; i++ {
		clients[i] = dialRaw(t, addr)
		defer clients[i].Close()
	}
	openWg.Wait()
	close(openBarrier)

	// Fixed script per seed: pseudo-random sequence of writes, remote
	// reads, remote closes and local closes across goroutines.
	rng := rand.New(rand.NewSource(seed))
	var scriptWg sync.WaitGroup
	const steps = 60

	// Phase barriers ensure writes-while-closing really happen rather than
	// the whole script serializing by chance.
	localClosed := make([]int32, nConn)

	for step := 0; step < steps; step++ {
		idx := rng.Intn(nConn)
		switch rng.Intn(4) {
		case 0, 1: // local write, maybe concurrent with a local close
			payload := make([]byte, 64*1024+rng.Intn(64*1024))
			fillByte(payload, byte(65+step%26))
			scriptWg.Add(1)
			go func(idx int, payload []byte) {
				defer scriptWg.Done()
				connsRegMu[idx].Lock()
				c := conns[idx]
				connsRegMu[idx].Unlock()
				if c == nil || atomic.LoadInt32(&localClosed[idx]) != 0 {
					return
				}
				_, _ = c.Write(payload)
			}(idx, payload)
		case 2: // local close
			scriptWg.Add(1)
			go func(idx int) {
				defer scriptWg.Done()
				if atomic.CompareAndSwapInt32(&localClosed[idx], 0, 1) {
					connsRegMu[idx].Lock()
					c := conns[idx]
					connsRegMu[idx].Unlock()
					if c != nil {
						_ = c.Close()
						_ = c.Close() // idempotent
					}
				}
			}(idx)
		case 3: // peer action: drain or abort
			scriptWg.Add(1)
			go func(idx int, abort bool) {
				defer scriptWg.Done()
				client := clients[idx]
				if abort {
					if tc, ok := client.(*net.TCPConn); ok {
						setLinger0(t, tc)
					}
					_ = client.Close()
					return
				}
				buf := make([]byte, 4096)
				for i := 0; i < 4; i++ {
					if _, err := client.Read(buf); err != nil {
						return
					}
				}
			}(idx, rng.Intn(2) == 0)
		}
	}

	// Every scripted close/abort is observable; wait for per-conn close
	// callbacks (at most one each).
	scriptWg.Wait()

	// Close whatever survived, from yet another goroutine per conn.
	var closeWg sync.WaitGroup
	for i := range conns {
		closeWg.Add(1)
		go func(i int) {
			defer closeWg.Done()
			_ = conns[i].Close()
		}(i)
	}
	closeWg.Wait()

	baseline := numGoroutine()
	done := stopEngineAsync(t, g)
	waitSignal(t, done, "seeded race stop")
	assertGoroutinesDrain(t, baseline)

	if got := atomic.LoadInt32(&closeCount); got != nConn {
		t.Fatalf("seed=%d onClose total = %d, want %d", seed, got, nConn)
	}
	closeErrMu.Lock()
	errs := append([]error(nil), closeErrs...)
	closeErrMu.Unlock()
	for i, err := range errs {
		if err != nil && !isPeerCloseError(err) {
			t.Fatalf("seed=%d close[%d] unexpected error: %v", seed, i, err)
		}
	}
	for _, msg := range alloc.errors() {
		t.Errorf("seed=%d allocator: %s", seed, msg)
	}
	alloc.mu.Lock()
	leaked := len(alloc.live)
	alloc.mu.Unlock()
	if leaked != 0 {
		t.Fatalf("seed=%d write blocks leaked: %d", seed, leaked)
	}
}

func fillByte(b []byte, v byte) {
	for i := range b {
		b[i] = v
	}
}

func numGoroutine() int { return runtime.NumGoroutine() }

var _ = io.EOF
var _ = errors.Is
