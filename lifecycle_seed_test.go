// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"fmt"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestLifecycleSeededRace is a repeatable race harness: a fixed seed
// produces the same operation sequence on a small set of loopback
// connections. Per-conn pause/resume signals deterministically force
// the server write queue past the socket send buffer and then drain it;
// the sequence also injects closes from both sides.
//
// It trips these mutation classes:
//   - write callback after buffer release / write-after-reuse: frame
//     integrity failures on the peer,
//   - double close: duplicated OnClose events,
//   - missed wakeup: a drain barrier never completes,
//   - Stop deadlock / partial poller wakeup: the stop watchdog fires.
func TestLifecycleSeededRace(t *testing.T) {
	installLeakCheck(t)
	t.Cleanup(resetLifecycleHooks)

	const (
		connNum     = 4
		opsPerConn  = 24
		frameBody   = 4 * 1024
		queueFrames = 24 // pause until >= this many frames are in flight
		seed        = int64(0x5eeded)
	)

	alloc := newTrackingAllocator(t)
	counters := newConnCounters()
	closeEventCh := make(chan *Conn, 64)
	var flushedBytes int64

	g, stop := newListeningEngine(t, Config{
		Name:          "lc-seed",
		NPoller:       3,
		BodyAllocator: alloc,
	}, func(g *Engine) {
		g.OnOpen(func(c *Conn) { counters.markOpen(c) })
		g.OnData(func(c *Conn, data []byte) {
			// Echo framed data; with the peer paused this queues.
			if _, err := c.Write(data); err != nil && !isLocalClosed(err) {
				seedFail("server echo write: %v", err)
			}
		})
		g.OnWrittenSize(func(c *Conn, b []byte, n int) {
			atomic.AddInt64(&flushedBytes, int64(n))
		})
		g.OnClose(func(c *Conn, err error) {
			counters.markClose(c)
			select {
			case closeEventCh <- c:
			default:
			}
		})
	})

	type peerState struct {
		conn net.Conn
		// pauseCh: empty => peer reader pauses; closed => drains.
		pauseCh chan struct{}
		// frames the writer expects the reader to have consumed.
		expectMu  sync.Mutex
		expect    int
		consumed  int
		drainCh   chan struct{} // closed when consumed == expect
		readerErr error
	}
	peers := make([]*peerState, connNum)
	for i := 0; i < connNum; i++ {
		conn, err := net.Dial("tcp", g.Addrs[0])
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetNoDelay(true)
			_ = tc.SetReadBuffer(4 * 1024)
		}
		ps := &peerState{conn: conn, pauseCh: make(chan struct{}), drainCh: make(chan struct{})}
		peers[i] = ps
	}

	waitOpens(t, counters, connNum)
	serverConns := snapshotServerConns(g, connNum)

	// One persistent reader per conn. It validates every echo frame;
	// when the writer pauses it stops reading until resume.
	var readerWg sync.WaitGroup
	for i, ps := range peers {
		readerWg.Add(1)
		tag := uint64(i + 1)
		go func(ps *peerState, tag uint64) {
			defer readerWg.Done()
			br := newFrameReader(ps.conn, 50*time.Millisecond)
			for {
				seq, body, err := br.readFrame()
				if err != nil {
					// Connection going away is expected; only record
					// unexpected error categories.
					if err != io.EOF && !isLocalClosed(err) && !isPeerClosedError(err) {
						if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
							ps.expectMu.Lock()
							ps.readerErr = err
							ps.expectMu.Unlock()
						}
					}
					return
				}
				if binary.BigEndian.Uint64(br.hdr[8:16]) != tag || !frameValid(seq, tag, body) {
					seedFail("frame corruption tag/seq=%d/%d", tag, seq)
					return
				}
				ps.expectMu.Lock()
				ps.consumed++
				if ps.drainCh != nil && ps.consumed >= ps.expect {
					close(ps.drainCh)
					ps.drainCh = nil
				}
				paused := ps.pauseCh
				ps.expectMu.Unlock()
				select {
				case <-paused:
				default:
				}
			}
		}(ps, tag)
	}

	// pause drains the peer reader and returns a resume func plus a
	// barrier that completes once n frames have been consumed.
	pause := func(ps *peerState) {
		ps.expectMu.Lock()
		ps.pauseCh = make(chan struct{})
		ps.expectMu.Unlock()
	}
	resume := func(ps *peerState, n int) {
		ps.expectMu.Lock()
		ps.expect = ps.consumed + n
		ps.drainCh = make(chan struct{})
		ch := ps.drainCh
		close(ps.pauseCh)
		ps.pauseCh = closedChan()
		ps.expectMu.Unlock()
		select {
		case <-ch:
		case <-time.After(lifecycleTestTimeout):
			seedFail("drain barrier stalled (missed wakeup), flushed=%d", atomic.LoadInt64(&flushedBytes))
		}
	}

	var seqNum uint64
	var wg sync.WaitGroup
	for i, sc := range serverConns {
		wg.Add(1)
		go func(idx int, c *Conn, ps *peerState, rng *rand.Rand) {
			defer wg.Done()
			pause(ps) // reader pauses from the start
			inFlight := 0
			for op := 0; op < opsPerConn; op++ {
				switch rng.Intn(12) {
				case 0:
					_ = c.CloseWithError(errors.New("seeded server close"))
					return
				case 1:
					_ = ps.conn.Close()
					return
				}

				seq := atomic.AddUint64(&seqNum, 1)
				frame := buildFrame(seq, uint64(idx+1), frameBody)
				if _, err := c.Write(frame); err != nil {
					if !isLocalClosed(err) {
						seedFail("server write: %v", err)
					}
					return
				}
				inFlight++

				if inFlight >= queueFrames {
					resume(ps, inFlight) // force flush cycle
					pause(ps)
					inFlight = 0
				}
			}
			resume(ps, inFlight) // flush residual
			_ = c.Close()
		}(i, sc, peers[i], rand.New(rand.NewSource(seed+int64(i+1)*7919)))
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("seeded sequence stalled, flushed=%d bytes (missed wakeup or Stop hazard)", atomic.LoadInt64(&flushedBytes))
	}

	for _, ps := range peers {
		_ = ps.conn.Close()
	}

	closed := map[*Conn]struct{}{}
	deadline := time.After(lifecycleTestTimeout)
	for len(closed) < connNum {
		select {
		case c := <-closeEventCh:
			closed[c] = struct{}{}
		case <-deadline:
			t.Fatalf("only %d/%d conns produced OnClose", len(closed), connNum)
		}
	}

	readerWg.Wait()
	stop()
	counters.assertOnceEach(t)
	alloc.assertBalanced(t)
	if err := seedErr(); err != nil {
		t.Fatal(err)
	}
	for _, ps := range peers {
		ps.expectMu.Lock()
		rerr := ps.readerErr
		ps.expectMu.Unlock()
		if rerr != nil {
			t.Fatalf("unexpected peer reader error: %v", rerr)
		}
	}
}

// waitOpens blocks until n OnOpen events have been observed.
func waitOpens(t *testing.T, counters *connCounters, n int) {
	deadline := time.Now().Add(lifecycleTestTimeout)
	for time.Now().Before(deadline) {
		if counters.totalOpens() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("only %d/%d server conns opened", counters.totalOpens(), n)
}

var closedOnce = make(chan struct{})

func init() {
	close(closedOnce)
}

//go:norace
func closedChan() chan struct{} { return closedOnce }

// ---- framed protocol: [8]seq [8]tag [n] body = byte(seq)^byte(i) ----

const (
	frameHeaderSize = 16
	frameBodySize   = 4 * 1024
)

//go:norace
func buildFrame(seq, tag uint64, bodyLen int) []byte {
	b := make([]byte, frameHeaderSize+bodyLen)
	binary.BigEndian.PutUint64(b[0:8], seq)
	binary.BigEndian.PutUint64(b[8:16], tag)
	for i := 0; i < bodyLen; i++ {
		b[frameHeaderSize+i] = byte(seq) ^ byte(i)
	}
	return b
}

//go:norace
func frameValid(seq, tag uint64, body []byte) bool {
	for i := 0; i < len(body); i++ {
		if body[i] != byte(seq)^byte(i) {
			return false
		}
	}
	return true
}

type frameReader struct {
	r        net.Conn
	hdr      [frameHeaderSize]byte
	body     []byte
	deadline time.Duration
}

//go:norace
func newFrameReader(r net.Conn, deadline time.Duration) *frameReader {
	return &frameReader{r: r, deadline: deadline}
}

//go:norace
func (f *frameReader) readFrame() (uint64, []byte, error) {
	_ = f.r.SetReadDeadline(time.Now().Add(f.deadline))
	if _, err := io.ReadFull(f.r, f.hdr[:]); err != nil {
		return 0, nil, err
	}
	if cap(f.body) < frameBodySize {
		f.body = make([]byte, frameBodySize)
	}
	body := f.body[:frameBodySize]
	if _, err := io.ReadFull(f.r, body); err != nil {
		return 0, nil, err
	}
	return binary.BigEndian.Uint64(f.hdr[0:8]), body, nil
}

// ---- shared failure signal ----

var (
	seedErrMu sync.Mutex
	seedErrV  error
)

func seedFail(format string, args ...interface{}) {
	seedErrMu.Lock()
	if seedErrV == nil {
		seedErrV = fmt.Errorf(format, args...)
	}
	seedErrMu.Unlock()
}

func seedErr() error {
	seedErrMu.Lock()
	defer seedErrMu.Unlock()
	return seedErrV
}

var _ = io.ErrUnexpectedEOF
