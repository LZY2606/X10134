package nbio

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

// lifecycleTestTimeout is only a liveness guard: every passing test is
// driven by channels and conditions and finishes in milliseconds.
const lifecycleTestTimeout = 10 * time.Second

func waitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timeout waiting for %s", what)
	}
}

func waitConn(t *testing.T, ch <-chan *Conn, what string) *Conn {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timeout waiting for %s", what)
		return nil
	}
}

func waitErr(t *testing.T, ch <-chan error, what string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(lifecycleTestTimeout):
		t.Fatalf("timeout waiting for %s", what)
		return nil
	}
}

// waitCondition waits until cond is true. It is a deterministic barrier,
// not a fixed sleep: it returns as soon as the condition is observable.
func waitCondition(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(lifecycleTestTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func mustDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return c
}

func stopEngine(t *testing.T, g *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("engine Stop did not return")
	}
}

func waitGoroutines(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(lifecycleTestTimeout)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, true)
			t.Fatalf("goroutines did not exit: baseline=%d current=%d\n%s",
				baseline, runtime.NumGoroutine(), buf[:n])
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// testAllocator is a mempool.Allocator that counts allocations/frees and
// detects double frees and leaks of write-cache buffers.
type testAllocator struct {
	mu      sync.Mutex
	mallocs int64
	frees   int64
	live    map[uintptr]struct{}
	err     error
}

func newTestAllocator() *testAllocator {
	return &testAllocator{live: map[uintptr]struct{}{}}
}

func (a *testAllocator) ptr(b []byte) uintptr {
	if len(b) == 0 {
		return 0
	}
	return uintptr(unsafe.Pointer(&b[0]))
}

func (a *testAllocator) Malloc(size int) *[]byte {
	b := make([]byte, size)
	p := a.ptr(b)
	a.mu.Lock()
	a.mallocs++
	if p != 0 {
		a.live[p] = struct{}{}
	}
	a.mu.Unlock()
	return &b
}

func (a *testAllocator) Realloc(pbuf *[]byte, size int) *[]byte {
	nb := a.Malloc(size)
	copy(*nb, *pbuf)
	a.Free(pbuf)
	return nb
}

func (a *testAllocator) Append(pbuf *[]byte, more ...byte) *[]byte {
	old := *pbuf
	oldPtr := a.ptr(old)
	nb := append(old, more...)
	newPtr := a.ptr(nb)
	if oldPtr != newPtr {
		a.mu.Lock()
		if oldPtr != 0 {
			delete(a.live, oldPtr)
		}
		if newPtr != 0 {
			a.live[newPtr] = struct{}{}
		}
		a.mu.Unlock()
	}
	return &nb
}

func (a *testAllocator) AppendString(pbuf *[]byte, more string) *[]byte {
	return a.Append(pbuf, []byte(more)...)
}

func (a *testAllocator) Free(pbuf *[]byte) {
	if pbuf == nil {
		return
	}
	p := a.ptr(*pbuf)
	a.mu.Lock()
	defer a.mu.Unlock()
	a.frees++
	if p == 0 {
		return
	}
	if _, ok := a.live[p]; !ok {
		if a.err == nil {
			a.err = fmt.Errorf("buffer freed twice or freed without allocation: %x", p)
		}
		return
	}
	delete(a.live, p)
}

func (a *testAllocator) mallocCount() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mallocs
}

func (a *testAllocator) checkBalanced(t *testing.T) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		t.Fatalf("allocator error: %v", a.err)
	}
	if len(a.live) != 0 {
		t.Fatalf("write buffers not returned to allocator: %d live (malloc=%d free=%d)",
			len(a.live), a.mallocs, a.frees)
	}
}

// connEvents records the per-connection callback order: "open", "data",
// "close". Events on the same connection must stay ordered and OnClose
// must be delivered exactly once.
type connEvents struct {
	mu  sync.Mutex
	seq []string
}

func (l *connEvents) add(event string) {
	l.mu.Lock()
	l.seq = append(l.seq, event)
	l.mu.Unlock()
}

func resetTestHooks() {
	testHooks.afterAccept = nil
	testHooks.afterAddConn = nil
	testHooks.stopBeforeConnSweep = nil
}

// newLifecycleEngine creates an unstarted engine; handlers must be
// registered before startLifecycleEngine is called.
func newLifecycleEngine(t *testing.T, alloc *testAllocator) *Engine {
	t.Helper()
	g := NewEngine(Config{
		Network:       "tcp",
		Addrs:         []string{"127.0.0.1:0"},
		NPoller:       1,
		BodyAllocator: alloc,
	})
	return g
}

func startLifecycleEngine(t *testing.T, g *Engine) {
	t.Helper()
	if err := g.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
}

// TestLifecycleCloseDuringOnOpen: another goroutine closes the connection
// while the user OnOpen callback has not returned yet. Close and OnClose
// must complete exactly once and with no error, and nothing must block.
func TestLifecycleCloseDuringOnOpen(t *testing.T) {
	baseline := runtime.NumGoroutine()
	g := newLifecycleEngine(t, nil)

	var opens, closes int32
	openEntered := make(chan *Conn, 1)
	openRelease := make(chan struct{})
	closeCh := make(chan error, 1)
	g.OnOpen(func(c *Conn) {
		atomic.AddInt32(&opens, 1)
		openEntered <- c
		<-openRelease
	})
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closes, 1)
		closeCh <- err
	})
	startLifecycleEngine(t, g)

	peer := mustDial(t, g.Addrs[0])
	defer peer.Close()
	c := waitConn(t, openEntered, "OnOpen")

	closeReturned := make(chan error, 1)
	go func() { closeReturned <- c.Close() }()
	if err := waitErr(t, closeReturned, "Close during OnOpen"); err != nil {
		t.Fatalf("Close during OnOpen: %v", err)
	}

	// OnClose is delivered even though OnOpen is still blocked.
	if err := waitErr(t, closeCh, "OnClose while OnOpen blocked"); err != nil {
		t.Fatalf("OnClose while OnOpen blocked: %v", err)
	}

	close(openRelease)

	// A second Close after OnOpen returned is a no-op.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	stopEngine(t, g)

	if n := atomic.LoadInt32(&opens); n != 1 {
		t.Fatalf("OnOpen called %d times, want 1", n)
	}
	if n := atomic.LoadInt32(&closes); n != 1 {
		t.Fatalf("OnClose called %d times, want 1", n)
	}
	waitGoroutines(t, baseline)
}

// TestLifecyclePeerCloseWithQueuedWrites: the Conn already holds several
// user-space write buffers when the peer closes. Every queued buffer must
// be returned to the allocator exactly once and OnClose must fire once.
func TestLifecyclePeerCloseWithQueuedWrites(t *testing.T) {
	baseline := runtime.NumGoroutine()
	alloc := newTestAllocator()
	g := newLifecycleEngine(t, alloc)

	var opens, closes int32
	openCh := make(chan *Conn, 1)
	closeCh := make(chan error, 1)
	g.OnOpen(func(c *Conn) {
		atomic.AddInt32(&opens, 1)
		openCh <- c
	})
	g.OnData(func(c *Conn, data []byte) {})
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closes, 1)
		closeCh <- err
	})
	startLifecycleEngine(t, g)

	peer := mustDial(t, g.Addrs[0])
	defer peer.Close()
	c := waitConn(t, openCh, "OnOpen")

	var drainDone chan struct{}
	if testConnHasWriteQueue {
		// Shrink both sides of the kernel pipe: user-space writes are
		// then guaranteed to be queued in the Conn's write list.
		if tcp, ok := peer.(*net.TCPConn); ok {
			_ = tcp.SetReadBuffer(4096)
		}
		_ = c.SetWriteBuffer(4096)
		for i := 0; i < 3; i++ {
			if _, err := c.Write(make([]byte, 512*1024)); err != nil {
				t.Fatalf("write %d: %v", i, err)
			}
		}
		// The peer never reads, so more than one write buffer must be
		// queued at this point.
		if n := alloc.mallocCount(); n < 2 {
			t.Fatalf("expected multiple queued write buffers, got %d", n)
		}
	} else {
		// The std poller writes synchronously and has no write queue:
		// keep the payload small and let the peer drain it.
		drainDone = make(chan struct{})
		go func() {
			_, _ = io.Copy(io.Discard, peer)
			close(drainDone)
		}()
		if _, err := c.Write([]byte("queued-write-close")); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	_ = peer.Close()

	if err := waitErr(t, closeCh, "OnClose after peer close"); err == nil {
		t.Fatalf("expected non-nil close error after peer closed, got nil")
	}
	if drainDone != nil {
		waitSignal(t, drainDone, "peer drain exit")
	}

	stopEngine(t, g)

	if n := atomic.LoadInt32(&opens); n != 1 {
		t.Fatalf("OnOpen called %d times, want 1", n)
	}
	if n := atomic.LoadInt32(&closes); n != 1 {
		t.Fatalf("OnClose called %d times, want 1", n)
	}
	alloc.checkBalanced(t)
	waitGoroutines(t, baseline)
}

// TestLifecycleAsyncWriteThenCloseInOnData: OnData triggers a write from
// another goroutine and closes the conn immediately afterwards. The write
// must either complete or observe ErrClosed, buffers must balance and all
// goroutines must exit.
func TestLifecycleAsyncWriteThenCloseInOnData(t *testing.T) {
	baseline := runtime.NumGoroutine()
	alloc := newTestAllocator()
	g := newLifecycleEngine(t, alloc)

	var opens, closes int32
	openCh := make(chan *Conn, 1)
	closeCh := make(chan error, 1)
	dataFired := make(chan struct{})
	var dataOnce sync.Once
	g.OnOpen(func(c *Conn) {
		atomic.AddInt32(&opens, 1)
		_ = c.SetWriteBuffer(4096)
		openCh <- c
	})
	g.OnData(func(c *Conn, data []byte) {
		dataOnce.Do(func() {
			defer close(dataFired)
			writeDone := make(chan error, 1)
			go func() {
				_, err := c.Write(make([]byte, 512*1024))
				writeDone <- err
			}()
			_ = c.Close()
			if err := <-writeDone; err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("async write returned unexpected error: %v", err)
			}
		})
	})
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closes, 1)
		closeCh <- err
	})
	startLifecycleEngine(t, g)

	peer := mustDial(t, g.Addrs[0])
	if tcp, ok := peer.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(4096)
	}
	defer peer.Close()
	waitConn(t, openCh, "OnOpen")

	if _, err := peer.Write([]byte("trigger")); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	waitSignal(t, dataFired, "OnData async write/close")

	if err := waitErr(t, closeCh, "OnClose after local close"); err != nil {
		t.Fatalf("OnClose error: %v", err)
	}

	stopEngine(t, g)

	if n := atomic.LoadInt32(&opens); n != 1 {
		t.Fatalf("OnOpen called %d times, want 1", n)
	}
	if n := atomic.LoadInt32(&closes); n != 1 {
		t.Fatalf("OnClose called %d times, want 1", n)
	}
	alloc.checkBalanced(t)
	waitGoroutines(t, baseline)
}

// TestLifecycleStopWithInflightAccept: a connection finishes accept while
// the engine is stopping. Hooks pin the exact order:
// accept completed -> Stop stops listeners -> conn registered -> Stop
// sweeps conns. The inflight conn must be closed exactly once by the
// sweep, with a nil error, and Stop must return.
func TestLifecycleStopWithInflightAccept(t *testing.T) {
	baseline := runtime.NumGoroutine()

	acceptEntered := make(chan struct{})
	acceptRelease := make(chan struct{})
	addConnDone := make(chan struct{})
	stopHookEntered := make(chan struct{})
	stopRelease := make(chan struct{})
	var acceptOnce, addOnce, stopHookOnce sync.Once
	testHooks.afterAccept = func() {
		acceptOnce.Do(func() {
			close(acceptEntered)
			<-acceptRelease
		})
	}
	testHooks.afterAddConn = func() {
		addOnce.Do(func() { close(addConnDone) })
	}
	testHooks.stopBeforeConnSweep = func() {
		stopHookOnce.Do(func() {
			close(stopHookEntered)
			<-stopRelease
		})
	}
	defer resetTestHooks()

	g := NewEngine(Config{
		Network: "tcp",
		Addrs:   []string{"127.0.0.1:0"},
		NPoller: 1,
	})
	var opens, closes, stops int32
	openCh := make(chan *Conn, 1)
	closeCh := make(chan error, 1)
	g.OnOpen(func(c *Conn) {
		atomic.AddInt32(&opens, 1)
		openCh <- c
	})
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closes, 1)
		closeCh <- err
	})
	g.OnStop(func() { atomic.AddInt32(&stops, 1) })
	dataCh := make(chan struct{})
	g.OnData(func(c *Conn, data []byte) {
		select {
		case <-dataCh:
		default:
			close(dataCh)
		}
	})
	if err := g.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	peer := mustDial(t, g.Addrs[0])
	defer peer.Close()
	waitSignal(t, acceptEntered, "accepted conn parked before addConn")

	stopReturned := make(chan struct{})
	go func() {
		g.Stop()
		close(stopReturned)
	}()
	waitSignal(t, stopHookEntered, "Stop parked before conn sweep")

	// Let the accepted conn finish addConn while Stop is in progress,
	// then let Stop proceed with the conn sweep.
	close(acceptRelease)
	waitConn(t, openCh, "OnOpen of inflight accepted conn")
	waitSignal(t, addConnDone, "inflight conn added to poller")
	// Wait until the poller has applied the conn's event registration
	// (proven by a delivered read event) before the sweep closes it, so
	// the close does not race the poller's event registration.
	if _, err := peer.Write([]byte{1}); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	waitSignal(t, dataCh, "inflight conn read event")
	close(stopRelease)

	waitSignal(t, stopReturned, "engine Stop")
	if err := waitErr(t, closeCh, "OnClose for inflight accepted conn"); err != nil {
		t.Fatalf("inflight conn closed with error %v, want nil", err)
	}

	if n := atomic.LoadInt32(&opens); n != 1 {
		t.Fatalf("OnOpen called %d times, want 1", n)
	}
	if n := atomic.LoadInt32(&closes); n != 1 {
		t.Fatalf("OnClose called %d times, want 1", n)
	}
	if n := atomic.LoadInt32(&stops); n != 1 {
		t.Fatalf("OnStop called %d times, want 1", n)
	}
	waitGoroutines(t, baseline)
}

// TestLifecycleReentrantCloseInOnClose: the user OnClose callback closes
// the conn again. The re-entrant Close must be a no-op: no panic, no
// deadlock and no second OnClose event.
func TestLifecycleReentrantCloseInOnClose(t *testing.T) {
	baseline := runtime.NumGoroutine()
	g := newLifecycleEngine(t, nil)

	var closes int32
	type closeResult struct {
		err       error
		reenterOk bool
	}
	resultCh := make(chan closeResult, 1)
	g.OnClose(func(c *Conn, err error) {
		atomic.AddInt32(&closes, 1)
		resultCh <- closeResult{err: err, reenterOk: c.Close() == nil}
	})

	openCh := make(chan *Conn, 1)
	g.OnOpen(func(c *Conn) { openCh <- c })
	dataCh := make(chan struct{})
	g.OnData(func(c *Conn, data []byte) {
		select {
		case <-dataCh:
		default:
			close(dataCh)
		}
	})
	startLifecycleEngine(t, g)

	peer := mustDial(t, g.Addrs[0])
	defer peer.Close()
	c := waitConn(t, openCh, "OnOpen")

	// Wait until the poller has applied the conn's event registration
	// (proven by a delivered read event) before closing, so Close does
	// not race the poller's event registration in the kernel.
	if _, err := peer.Write([]byte{1}); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	waitSignal(t, dataCh, "conn read event")

	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	var res closeResult
	select {
	case res = <-resultCh:
	case <-time.After(lifecycleTestTimeout):
		t.Fatal("timeout waiting for OnClose")
	}
	if res.err != nil {
		t.Fatalf("OnClose error: %v", res.err)
	}
	if !res.reenterOk {
		t.Fatal("re-entrant Close inside OnClose returned an error")
	}
	// Closing again from outside must stay a no-op.
	if err := c.Close(); err != nil {
		t.Fatalf("second Close after OnClose: %v", err)
	}

	stopEngine(t, g)
	if n := atomic.LoadInt32(&closes); n != 1 {
		t.Fatalf("OnClose called %d times, want 1", n)
	}
	waitGoroutines(t, baseline)
}

// Deterministic pseudo-random operation script, replayed on a handful of
// connections. The seed is fixed, so the interleaving candidates are
// reproducible run over run; scheduling itself is still up to the runtime
// and the race detector.
const (
	raceOpWrite = iota
	raceOpWritev
	raceOpExecuteWrite
	raceOpPeerWrite
	raceOpYield
	raceOpKindNum
)

type raceOp struct {
	kind int
	size int
}

func racePatternByte(connIdx int, off int64) byte {
	return byte((connIdx*131 + int(off&0x7FFF)) % 251)
}

func raceFillPattern(buf []byte, connIdx int, off int64) {
	for i := range buf {
		buf[i] = racePatternByte(connIdx, off+int64(i))
	}
}

const (
	raceCloseServerThenPeer = iota
	raceClosePeerOnly
	raceCloseBothConcurrent
	raceCloseServerWithQueue
)

type raceConnScript struct {
	idx       int
	srv       *Conn
	cli       net.Conn
	ops       []raceOp
	drain     bool
	bigQueue  bool
	closeKind int
	exp       *raceExpectation

	queued int64 // pattern bytes handed to nbio by the driver
	sent   int64 // bytes the peer sent to the server
}

// raceReportSeg describes a range of the accepted write stream that nbio
// will report through OnWrittenSize.
type raceReportSeg struct {
	off int64
	len int64
}

// raceExpectation models which bytes of the accepted write stream nbio
// reports via OnWrittenSize, so the test can verify their content. Note
// that nbio does not report buffers of a writev that was written in full,
// so the driver adjusts segment lengths after each call.
type raceExpectation struct {
	mu   sync.Mutex
	segs []raceReportSeg
	pos  int
	used int64
}

func (e *raceExpectation) add(off, length int64) int {
	e.mu.Lock()
	e.segs = append(e.segs, raceReportSeg{off: off, len: length})
	e.mu.Unlock()
	return len(e.segs) - 1
}

func (e *raceExpectation) setLen(i int, length int64) {
	e.mu.Lock()
	e.segs[i].len = length
	e.mu.Unlock()
}

func (e *raceExpectation) check(connIdx int, b []byte) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, v := range b {
		for e.pos < len(e.segs) && (e.segs[e.pos].len == 0 || e.used == e.segs[e.pos].len) {
			e.pos++
			e.used = 0
		}
		if e.pos >= len(e.segs) {
			return false
		}
		if v != racePatternByte(connIdx, e.segs[e.pos].off+e.used) {
			return false
		}
		e.used++
	}
	return true
}

func (e *raceExpectation) unconsumed() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var left int64
	for i := e.pos; i < len(e.segs); i++ {
		left += e.segs[i].len
	}
	return left - e.used
}

// TestLifecycleScriptedRace replays a fixed-seed operation script over a
// few connections and checks the lifecycle invariants: per-connection
// event order, exactly-once OnClose, write-buffer balance, no corrupted
// write callbacks, and a Stop that wakes every poller and returns.
func TestLifecycleScriptedRace(t *testing.T) {
	baseline := runtime.NumGoroutine()
	alloc := newTestAllocator()

	const connNum = 4
	g := NewEngine(Config{
		Network:       "tcp",
		Addrs:         []string{"127.0.0.1:0"},
		NPoller:       2,
		BodyAllocator: alloc,
	})

	var idxMu sync.Mutex
	idxOf := map[*Conn]int{}
	eventLogs := map[*Conn]*connEvents{}
	recvBytes := make([]int64, connNum)
	exps := make([]*raceExpectation, connNum)
	var closeCount int64
	var patternErrors int64
	openCh := make(chan *Conn, connNum)

	g.OnOpen(func(c *Conn) {
		l := &connEvents{}
		l.add("open")
		idxMu.Lock()
		eventLogs[c] = l
		idxMu.Unlock()
		openCh <- c
	})
	g.OnData(func(c *Conn, data []byte) {
		idxMu.Lock()
		l := eventLogs[c]
		idx := idxOf[c]
		idxMu.Unlock()
		// Record the event before publishing the byte count so that the
		// driver's recv barrier also orders the event log.
		l.add("data")
		atomic.AddInt64(&recvBytes[idx], int64(len(data)))
	})
	g.OnWrittenSize(func(c *Conn, b []byte, n int) {
		idxMu.Lock()
		idx := idxOf[c]
		idxMu.Unlock()
		if !exps[idx].check(idx, b[:n]) {
			atomic.AddInt64(&patternErrors, 1)
		}
	})
	g.OnClose(func(c *Conn, err error) {
		idxMu.Lock()
		l := eventLogs[c]
		idxMu.Unlock()
		l.add("close")
		atomic.AddInt64(&closeCount, 1)
	})
	if err := g.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	rng := rand.New(rand.NewSource(20260919))
	genOps := func(n, maxSize int) []raceOp {
		ops := make([]raceOp, n)
		for i := range ops {
			ops[i] = raceOp{kind: rng.Intn(raceOpKindNum), size: 1 + rng.Intn(maxSize)}
		}
		return ops
	}
	bigOps := func() []raceOp {
		return []raceOp{
			{kind: raceOpWrite, size: 256*1024 + rng.Intn(64*1024)},
			{kind: raceOpWritev, size: 128*1024 + rng.Intn(64*1024)},
			{kind: raceOpPeerWrite, size: 16},
			{kind: raceOpExecuteWrite, size: 128*1024 + rng.Intn(64*1024)},
			{kind: raceOpYield},
			{kind: raceOpWrite, size: 256*1024 + rng.Intn(64*1024)},
		}
	}

	scripts := make([]*raceConnScript, connNum)
	for i := 0; i < connNum; i++ {
		exps[i] = &raceExpectation{}
		cli := mustDial(t, g.Addrs[0])
		srv := waitConn(t, openCh, "OnOpen")
		idxMu.Lock()
		idxOf[srv] = i
		idxMu.Unlock()
		scripts[i] = &raceConnScript{idx: i, srv: srv, cli: cli, exp: exps[i]}
	}
	scripts[0].ops = genOps(12, 32*1024)
	scripts[0].drain = true
	scripts[0].closeKind = raceCloseServerThenPeer
	scripts[1].ops = bigOps()
	scripts[1].bigQueue = true
	scripts[1].closeKind = raceClosePeerOnly
	scripts[2].ops = genOps(12, 32*1024)
	scripts[2].drain = true
	scripts[2].closeKind = raceCloseBothConcurrent
	scripts[3].ops = bigOps()
	scripts[3].bigQueue = true
	scripts[3].closeKind = raceCloseServerWithQueue

	var wg sync.WaitGroup
	for _, sc := range scripts {
		sc := sc
		wg.Add(1)
		go func() {
			defer wg.Done()
			runRaceScript(t, g, sc, recvBytes)
		}()
	}
	wg.Wait()

	waitCondition(t, "all conns closed", func() bool {
		return atomic.LoadInt64(&closeCount) == connNum
	})

	stopEngine(t, g)

	for _, sc := range scripts {
		_ = sc.cli.Close()
	}

	// Per-connection events must be ordered: open first, close once, last.
	for _, sc := range scripts {
		idxMu.Lock()
		l := eventLogs[sc.srv]
		idxMu.Unlock()
		seq := l.seq
		if len(seq) == 0 || seq[0] != "open" {
			t.Fatalf("conn %d: events must start with open, got %v", sc.idx, seq)
		}
		nclose := 0
		for i, e := range seq {
			if e == "close" {
				nclose++
				if i != len(seq)-1 {
					t.Fatalf("conn %d: event after close: %v", sc.idx, seq)
				}
			}
		}
		if nclose != 1 {
			t.Fatalf("conn %d: OnClose delivered %d times: %v", sc.idx, nclose, seq)
		}
	}

	if n := atomic.LoadInt64(&patternErrors); n != 0 {
		t.Fatalf("write callback observed corrupted/reused buffers %d times", n)
	}
	for _, sc := range scripts {
		if sc.drain {
			if left := sc.exp.unconsumed(); left != 0 {
				t.Fatalf("conn %d: %d reported bytes missing from write callbacks", sc.idx, left)
			}
		}
	}
	alloc.checkBalanced(t)
	waitGoroutines(t, baseline)
}

func runRaceScript(t *testing.T, g *Engine, sc *raceConnScript, recvBytes []int64) {
	t.Helper()

	if sc.bigQueue && testConnHasWriteQueue {
		if tcp, ok := sc.cli.(*net.TCPConn); ok {
			_ = tcp.SetReadBuffer(4096)
		}
		_ = sc.srv.SetWriteBuffer(4096)
	}

	writeChunk := func(size int) {
		buf := make([]byte, size)
		raceFillPattern(buf, sc.idx, sc.queued)
		seg := sc.exp.add(sc.queued, int64(size))
		n, err := sc.srv.Write(buf)
		// Write reports the bytes accepted by nbio: on success that is
		// the whole buffer (the unflushed part is queued internally);
		// on a hard failure it is the partial amount already written.
		// -1 is returned when the conn is closed or the cache overflows.
		accepted := int64(acceptedWrite(n, err, size))
		sc.exp.setLen(seg, accepted)
		sc.queued += accepted
	}

	for _, op := range sc.ops {
		switch op.kind {
		case raceOpWrite:
			writeChunk(op.size)
		case raceOpWritev:
			parts := make([][]byte, 3)
			var off int64
			for i := range parts {
				n := op.size/3 + 1
				parts[i] = make([]byte, n)
				raceFillPattern(parts[i], sc.idx, sc.queued+off)
				off += int64(n)
			}
			total := int(off)
			seg := sc.exp.add(sc.queued, int64(total))
			n, err := sc.srv.Writev(parts)
			sc.queued += int64(acceptedWritev(n, err, total))
			sc.exp.setLen(seg, int64(reportedWritev(n, err, total)))
		case raceOpExecuteWrite:
			buf := make([]byte, op.size)
			raceFillPattern(buf, sc.idx, sc.queued)
			seg := sc.exp.add(sc.queued, int64(op.size))
			var n int
			var err error
			if sc.srv.Execute(func() { n, err = sc.srv.Write(buf) }) {
				accepted := int64(acceptedWrite(n, err, op.size))
				sc.exp.setLen(seg, accepted)
				sc.queued += accepted
			} else {
				sc.exp.setLen(seg, 0)
			}
		case raceOpPeerWrite:
			b := make([]byte, 1+op.size%64)
			if n, err := sc.cli.Write(b); err == nil {
				sc.sent += int64(n)
			}
		case raceOpYield:
			runtime.Gosched()
		}
	}

	// Always finish the write phase with a peer write: once the server
	// has received it, the poller has also applied every pending event
	// registration for this conn (changes are applied by the same
	// kevent call that delivers the read event), so the close phase
	// below cannot race the poller's event registration in the kernel.
	if n, err := sc.cli.Write([]byte{0}); err == nil {
		sc.sent += int64(n)
	}

	// The server must have received everything the peer sent before the
	// close phase starts, so the per-conn event order stays meaningful.
	waitCondition(t, "server received peer bytes", func() bool {
		return atomic.LoadInt64(&recvBytes[sc.idx]) == sc.sent
	})

	if sc.drain {
		if err := raceReadBack(sc); err != nil {
			t.Errorf("conn %d: %v", sc.idx, err)
		}
	}

	switch sc.closeKind {
	case raceCloseServerThenPeer:
		_ = sc.srv.Close()
		_ = sc.cli.Close()
		// Double close must be a no-op.
		_ = sc.srv.Close()
	case raceClosePeerOnly:
		_ = sc.cli.Close()
	case raceCloseBothConcurrent:
		go func() { _ = sc.cli.Close() }()
		_ = sc.srv.Close()
	case raceCloseServerWithQueue:
		_ = sc.srv.Close()
		_ = sc.cli.Close()
	}
}

// acceptedWrite maps Conn.Write's return values to the number of bytes
// nbio took ownership of (written or queued): the unflushed remainder of
// a successful Write is queued internally, so a nil error means the whole
// buffer was accepted.
func acceptedWrite(n int, err error, size int) int {
	if err == nil {
		return size
	}
	if n > 0 {
		return n
	}
	return 0
}

// acceptedWritev maps Conn.Writev's return values to the number of bytes
// nbio took ownership of. A partial writev queues the remainder, so a nil
// error means the whole input was accepted; an EAGAIN with nothing written
// means the input was dropped by nbio.
func acceptedWritev(n int, err error, size int) int {
	switch {
	case err == nil:
		return size
	case errors.Is(err, syscall.EAGAIN):
		if n > 0 {
			return size
		}
		return 0
	default:
		if n > 0 {
			return n
		}
		return 0
	}
}

// reportedWritev maps Conn.Writev's return values to the number of bytes
// nbio reports through OnWrittenSize. nbio reports partial writev results
// (and the queued remainder later, when flushing); whether buffers of a
// fully written writev are reported depends on the poller (see
// testWritevReportsFull).
func reportedWritev(n int, err error, size int) int {
	switch {
	case err == nil:
		if n == size {
			if testWritevReportsFull {
				return size
			}
			return 0
		}
		return size
	case errors.Is(err, syscall.EAGAIN):
		if n > 0 {
			return size
		}
		return 0
	default:
		if n > 0 {
			return n
		}
		return 0
	}
}

// raceReadBack reads and verifies every patterned byte the server queued,
// which fails if a writable-event wakeup was lost.
func raceReadBack(sc *raceConnScript) error {
	buf := make([]byte, 32*1024)
	var off int64
	deadline := time.Now().Add(lifecycleTestTimeout)
	for off < sc.queued {
		_ = sc.cli.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, err := sc.cli.Read(buf)
		for j := 0; j < n; j++ {
			if buf[j] != racePatternByte(sc.idx, off+int64(j)) {
				return fmt.Errorf("corrupted byte at offset %d", off+int64(j))
			}
		}
		off += int64(n)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if time.Now().After(deadline) {
					return fmt.Errorf("received %d/%d bytes, missing wakeup?", off, sc.queued)
				}
				continue
			}
			return err
		}
	}
	return nil
}
