// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Scenario 1: another goroutine requests Close while OnOpen has not
// returned. Racing Closes must publish OnClose exactly once, the first
// error wins, and the peer observes a clean EOF.
func TestLifecycleCloseWhileOnOpenPending(t *testing.T) {
	h := newHarness(t)
	enter := make(chan struct{})
	proceed := make(chan struct{})
	var onceEnter sync.Once
	h.onOpen = func(c *Conn) {
		// OnOpen has entered (open event already published); park here so
		// the callback has not returned when another goroutine Closes.
		onceEnter.Do(func() { close(enter) })
		<-proceed
	}
	h.start()

	raw, err := net.Dial("tcp", h.g.Addrs[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	<-enter
	c := h.waitOpen()

	closedRet := make(chan error, 2)
	go func() { closedRet <- c.Close() }()
	go func() { closedRet <- c.CloseWithError(errors.New("user close during open")) }()

	// Let both racing Closes block on the conn mutex before OnOpen returns.
	runtime.Gosched()
	runtime.Gosched()
	close(proceed)

	got := h.waitClose(c)
	h.assertSingleClose(c)
	assertCloseErrCategory(t, got)
	if closed, err := c.IsClosed(); !closed {
		t.Fatalf("conn not marked closed")
	} else if err != nil {
		assertCloseErrCategory(t, err)
	}

	if _, err := raw.Read(make([]byte, 8)); err != io.EOF {
		t.Fatalf("peer read err = %v, want EOF", err)
	}
	_ = raw.Close()

	h.stopAndClean()
}

// Scenario 3 (cross platform): OnData triggers a foreign write and
// immediately closes. Write-after-close fails with net.ErrClosed; OnClose
// fires once and the write cache (if any) is freed once.
func TestLifecycleAsyncWriteThenCloseInOnData(t *testing.T) {
	h := newHarness(t)
	h.onData = func(c *Conn, data []byte) {
		go func() {
			_, werr := c.Write(append([]byte{}, data...))
			if werr != nil && !errors.Is(werr, net.ErrClosed) {
				t.Errorf("foreign write err = %v, want nil or ErrClosed", werr)
			}
		}()
		_ = c.Close()
	}
	h.start()

	raw, c := h.dialServer()
	defer raw.Close()
	if _, err := raw.Write([]byte("ping")); err != nil {
		t.Fatalf("peer write: %v", err)
	}

	h.waitClose(c)
	h.assertSingleClose(c)
	h.assertBalanced()

	h.stopAndClean()
}

// Scenario 4: a new connection completes accept while Engine is Stopping.
// The accepted conn must still receive one OnOpen and one OnClose, and Stop
// must not hang on partial poller wakeup or miss the conn.
func TestLifecycleAcceptWhileStopping(t *testing.T) {
	h := newHarness(t)
	h.npoller = 4
	var bl *barrierListener
	h.listenFn = func(network, addr string) (net.Listener, error) {
		_, reserved := freeListenAddr(t)
		bl = newBarrierListener(reserved)
		return bl, nil
	}
	registered := make(chan struct{}, 1)
	var onceReg sync.Once
	h.hooks = &testHooks{
		connAdded: func(c *Conn) {
			onceReg.Do(func() { close(registered) })
		},
	}
	h.start()

	addr := h.g.Addrs[0]

	// Dial first so a conn is sitting in the listener backlog.
	raw, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	// The listener goroutine accepts it and parks inside Accept.
	<-bl.acceptReady

	stopDone := make(chan struct{})
	go func() {
		h.g.Stop()
		close(stopDone)
	}()

	// Release the parked Accept: its conn runs addConn while Stop is
	// closing listeners; Close waits for addConn to finish.
	bl.releaseParkedAccept()
	<-registered
	c := <-h.openCh
	_ = raw

	select {
	case <-stopDone:
	case <-time.After(lifecycleWait):
		t.Fatalf("Stop hung with in-flight accept")
	}

	h.assertSingleClose(c)
	assertCloseErrCategory(t, h.recordFor(c).lastErr())
	h.assertBalanced()
	h.waitGoroutinesGone()
}

// Scenario 5: the user calls Close again from inside OnClose. The close
// path must complete once and goroutines must still exit.
func TestLifecycleReentrantCloseInOnClose(t *testing.T) {
	for _, peerFirst := range []bool{false, true} {
		name := "user-close"
		if peerFirst {
			name = "peer-close"
		}
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			reentered := make(chan struct{})
			h.onClose = func(c *Conn, err error) {
				select {
				case <-reentered:
				default:
					close(reentered)
					_ = c.Close()
					_ = c.CloseWithError(nil)
					go func() { _ = c.Close() }()
				}
			}
			h.start()

			raw := netDial(t, h.g.Addrs[0])
			c := h.waitOpen()

			if peerFirst {
				_ = raw.Close()
			} else {
				go func() { _ = c.Close() }()
			}

			<-reentered
			h.waitClose(c)
			h.assertSingleClose(c)
			assertCloseErrCategory(t, h.recordFor(c).lastErr())
			if !peerFirst {
				_ = raw.Close()
			}
			h.stopAndClean()
		})
	}
}

// Deterministic churn over a small, fixed connection set: foreign writes,
// reconnects and closes follow a fixed sequence. Peer payload bytes stay in
// the valid color range (a poison 0xDD byte means reuse-after-free), callback
// counters detect double close, Stop completion detects missed poller
// wakeup. Cross platform.
func TestLifecycleConcurrentChurn(t *testing.T) {
	h := newHarness(t)
	h.npoller = 3
	const nconns = 4
	h.start()

	type slot struct {
		mu   sync.Mutex
		raw  net.Conn
		c    *Conn
		live bool
	}
	slots := make([]*slot, nconns)
	for i := range slots {
		slots[i] = &slot{}
	}

	connect := func(i int) {
		s := slots[i]
		raw, c := h.dialServer()
		s.mu.Lock()
		s.raw = raw
		s.c = c
		s.live = true
		s.mu.Unlock()

		go func() {
			rb := make([]byte, 4096)
			for {
				n, err := raw.Read(rb)
				for _, b := range rb[:n] {
					if b == lifecyclePoison || b == 0 || b > 200 {
						t.Errorf("peer received invalid byte 0x%x (poison/reuse?)", b)
						return
					}
				}
				if err != nil {
					return
				}
			}
		}()
	}
	for i := range slots {
		connect(i)
	}

	seq := fixedSequence(1)
	for step := 0; step < 120; step++ {
		i := int(seq() % nconns)
		s := slots[i]
		s.mu.Lock()
		if !s.live {
			s.mu.Unlock()
			connect(i)
			continue
		}
		c := s.c
		s.mu.Unlock()

		switch seq() % 4 {
		case 0, 1:
			col := byte(1 + seq()%200)
			payload := make([]byte, 1+int(seq()%256))
			for k := range payload {
				payload[k] = col
			}
			go func() { _, _ = c.Write(payload) }()
		case 2:
			go func() { _ = c.Close() }()
			s.mu.Lock()
			s.live = false
			_ = s.raw.Close()
			s.mu.Unlock()
		case 3:
			col := byte(1 + seq()%200)
			payload := make([]byte, 128)
			for k := range payload {
				payload[k] = col
			}
			go func() { _, _ = c.Write(payload) }()
			go func() { _ = c.Close() }()
			s.mu.Lock()
			s.live = false
			_ = s.raw.Close()
			s.mu.Unlock()
		}
	}

	for _, s := range slots {
		s.mu.Lock()
		if s.live {
			s.live = false
			_ = s.raw.Close()
		}
		s.mu.Unlock()
	}

	h.stopAndClean()
}

func netDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}
