package nbio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/lesismal/nbio/mempool"
)

// ---------------------------------------------------------------------------
// tracking allocator: every queued write buffer must be freed exactly once and
// must never be touched after release.
// ---------------------------------------------------------------------------

var freedCanary = []byte("NBIO_FREED_BUFFER_CANARY")

type trackedAllocRecord struct {
	live   bool
	data   []byte
	freedN int
}

type trackingAllocator struct {
	mux     sync.Mutex
	records map[uintptr]*trackedAllocRecord
	mallocN int64
	freeN   int64
}

func newTrackingAllocator() *trackingAllocator {
	return &trackingAllocator{records: map[uintptr]*trackedAllocRecord{}}
}

func unsafeDataPtr(pbuf *[]byte) uintptr {
	return (*sliceHeader)(unsafe.Pointer(pbuf)).data
}

type sliceHeader struct {
	data uintptr
	len  int
	cap  int
}

func (a *trackingAllocator) Malloc(size int) *[]byte {
	buf := make([]byte, size)
	pbuf := &buf
	a.mux.Lock()
	a.mallocN++
	if size > 0 {
		a.records[unsafeDataPtr(pbuf)] = &trackedAllocRecord{live: true, data: buf}
	}
	a.mux.Unlock()
	return pbuf
}

func (a *trackingAllocator) Realloc(pbuf *[]byte, size int) *[]byte {
	if size <= cap(*pbuf) {
		*pbuf = (*pbuf)[:size]
		return pbuf
	}
	nb := a.Malloc(size)
	copy(*nb, *pbuf)
	a.Free(pbuf)
	return nb
}

func (a *trackingAllocator) Append(pbuf *[]byte, more ...byte) *[]byte {
	if cap(*pbuf)-len(*pbuf) >= len(more) {
		*pbuf = append(*pbuf, more...)
		return pbuf
	}
	nb := a.Malloc(len(*pbuf) + len(more))
	copy(*nb, *pbuf)
	copy((*nb)[len(*pbuf):], more)
	a.Free(pbuf)
	return nb
}

func (a *trackingAllocator) AppendString(pbuf *[]byte, more string) *[]byte {
	return a.Append(pbuf, []byte(more)...)
}

func (a *trackingAllocator) Free(pbuf *[]byte) {
	a.mux.Lock()
	defer a.mux.Unlock()
	a.freeN++
	if cap(*pbuf) == 0 {
		return
	}
	key := unsafeDataPtr(pbuf)
	r := a.records[key]
	if r == nil {
		panic(fmt.Sprintf("trackingAllocator: Free of unallocated buffer %x", key))
	}
	if !r.live {
		r.freedN++
		panic(fmt.Sprintf("trackingAllocator: buffer double free (freed %d times)", r.freedN))
	}
	copy(*pbuf, freedCanary)
	r.live = false
}

// assertNoLiveReuse verifies no live buffer has been stamped with the release
// canary of a freed allocation.
func (a *trackingAllocator) assertNoLiveReuse(t *testing.T) {
	t.Helper()
	a.mux.Lock()
	defer a.mux.Unlock()
	for key, r := range a.records {
		if r.live && bytes.Contains(r.data, freedCanary) {
			t.Fatalf("trackingAllocator: live buffer %x contains freed canary: use after free", key)
		}
	}
}

func (a *trackingAllocator) assertBalanced(t *testing.T) {
	t.Helper()
	a.mux.Lock()
	defer a.mux.Unlock()
	live := 0
	for _, r := range a.records {
		if r.live {
			live++
		}
	}
	if a.mallocN != a.freeN || live != 0 {
		t.Fatalf("trackingAllocator unbalanced: malloc=%d free=%d live=%d", a.mallocN, a.freeN, live)
	}
}

func (a *trackingAllocator) counts() (malloc, free, live int64) {
	a.mux.Lock()
	defer a.mux.Unlock()
	malloc = a.mallocN
	free = a.freeN
	for _, r := range a.records {
		if r.live {
			live++
		}
	}
	return
}

// ---------------------------------------------------------------------------
// engine harness
// ---------------------------------------------------------------------------

type lifecycleCounters struct {
	openN    int64
	closeN   int64
	writtenN int64
	mux      sync.Mutex
	closeCh  chan *Conn
	closeErr []error
}

func newCounters() *lifecycleCounters {
	return &lifecycleCounters{closeCh: make(chan *Conn, 256)}
}

func (lc *lifecycleCounters) recordClose(c *Conn, err error) {
	atomic.AddInt64(&lc.closeN, 1)
	lc.mux.Lock()
	lc.closeErr = append(lc.closeErr, err)
	lc.mux.Unlock()
	select {
	case lc.closeCh <- c:
	default:
	}
}

func (lc *lifecycleCounters) waitCloseCount(t *testing.T, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if atomic.LoadInt64(&lc.closeN) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("OnClose count = %d, want %d", atomic.LoadInt64(&lc.closeN), want)
}

func (lc *lifecycleCounters) closeErrors() []error {
	lc.mux.Lock()
	defer lc.mux.Unlock()
	out := make([]error, len(lc.closeErr))
	copy(out, lc.closeErr)
	return out
}

func newLifecycleEngine(t *testing.T, nPoller int, alloc mempool.Allocator) (*Engine, *lifecycleCounters) {
	t.Helper()
	if nPoller < 1 {
		nPoller = 1
	}
	g := NewEngine(Config{
		Network:       "tcp",
		Addrs:         []string{"127.0.0.1:0"},
		NPoller:       nPoller,
		BodyAllocator: alloc,
	})
	lc := newCounters()
	g.OnOpen(func(c *Conn) { atomic.AddInt64(&lc.openN, 1) })
	g.OnClose(func(c *Conn, err error) { lc.recordClose(c, err) })
	g.OnWrittenSize(func(c *Conn, b []byte, n int) {
		atomic.AddInt64(&lc.writtenN, int64(n))
	})
	g.OnData(func(c *Conn, data []byte) {
		hookOnDataMagic(c, data)
	})
	if err := g.Start(); err != nil {
		t.Fatalf("engine start: %v", err)
	}
	return g, lc
}

// isPeerCloseError classifies errors delivered to OnClose: a clean peer shutdown
// arrives as io.EOF; writes/flushes after the peer resets arrive as EPIPE or
// ECONNRESET. Locally requested closes carry the user-supplied error (often nil)
// and must never be classified as peer errors.
func isPeerCloseError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) {
		return true
	}
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET)
}

func waitChannel(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("timeout waiting for: %s", msg)
	}
}

func stopEngine(t *testing.T, g *Engine) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		g.Stop()
		close(done)
	}()
	waitChannel(t, done, 5*time.Second, "Engine.Stop to return")
}

// waitGoroutinesExit fails if a goroutine whose stack matches any substring is
// still running after the engine stops. It polls briefly instead of relying on
// sleeps, so it only treats actually-stuck goroutines as leaks.
func waitGoroutinesExit(t *testing.T, substrings ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		stack := string(buf[:n])
		bad := false
		for _, sub := range substrings {
			if strings.Contains(stack, sub) {
				bad = true
				break
			}
		}
		if !bad {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	t.Fatalf("leftover goroutine(s) remain:\n%s", string(buf[:n]))
}

// hookGuard installs test-only synchronization hooks for the duration of the
// test. Individual hooks are optional; the restore runs in cleanup so nested
// tests never inherit each other's barriers.
type hookGuard struct {
	t *testing.T
}

func installHooks(t *testing.T) *hookGuard {
	return &hookGuard{t: t}
}

func (h *hookGuard) restore() {
	hookOnOpenEnter = nil
	hookWriteQueued = nil
	hookStopAfterListeners = nil
	hookStopAfterConnsSnapshot = nil
	hookStopAfterWgConnWait = nil
	hookStopConnIter = nil
	hookConnPending = nil
	hookTargets.Range(func(key, _ interface{}) bool {
		hookTargets.Delete(key)
		return true
	})
	hookAddrTargets.Range(func(key, _ interface{}) bool {
		hookAddrTargets.Delete(key)
		return true
	})
	hookMagicTargets.Range(func(key, _ interface{}) bool {
		hookMagicTargets.Delete(key)
		return true
	})
}

// ---------------------------------------------------------------------------
// shared peer helpers
// ---------------------------------------------------------------------------

// dialPeer creates a real loopback TCP connection accepted by the engine and
// returns both the raw peer side and the nbio server side.
func dialPeer(t *testing.T, g *Engine) (net.Conn, *Conn) {
	t.Helper()
	peer, err := net.Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatalf("dial engine: %v", err)
	}
	nbc, err := g.AddConn(peer)
	if err != nil {
		_ = peer.Close()
		t.Fatalf("add conn: %v", err)
	}
	return peer, nbc
}

// dialPeerOffEngine creates the accepted server Conn through a throwaway
// listener and registers it directly with AddConn, so blocking engine
// callbacks never interact with stray connections from other sources.
func dialPeerOffEngine(t *testing.T, g *Engine) (net.Conn, *Conn) {
	t.Helper()
	l, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("temp listen: %v", err)
	}
	raw, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		_ = l.Close()
		t.Fatalf("temp dial: %v", err)
	}
	nbc, err := g.AddConn(raw)
	if err != nil {
		_ = raw.Close()
		_ = l.Close()
		t.Fatalf("add conn: %v", err)
	}
	return tempListenerConn{Conn: raw, l: l}, nbc
}

type tempListenerConn struct {
	net.Conn
	l net.Listener
}

func (c tempListenerConn) Close() error {
	err := c.Conn.Close()
	_ = c.l.Close()
	return err
}

func mustSetSmallSendBuffer(t *testing.T, c net.Conn) {
	t.Helper()
	sc, ok := c.(*net.TCPConn)
	if !ok {
		t.Fatalf("not a tcp conn: %T", c)
	}
	if err := sc.SetWriteBuffer(4096); err != nil {
		t.Fatalf("set write buffer: %v", err)
	}
	if err := sc.SetNoDelay(false); err != nil {
		t.Fatalf("set nodelay: %v", err)
	}
}

// ===========================================================================
// Scenario 1: another goroutine closes a connection while its OnOpen handler
// has not returned yet.
// ===========================================================================

func TestCloseDuringOnOpen(t *testing.T) {
	g, lc := newLifecycleEngine(t, 2, mempool.NewSTD())
	h := installHooks(t)
	defer h.restore()

	insideOpen := make(chan struct{})
	releaseOpen := make(chan struct{})
	var insideOnce sync.Once
	pendingSeen := make(chan *Conn, 4)
	hookConnPending = func(c *Conn) {
		select {
		case pendingSeen <- c:
		default:
		}
	}
	hookOnOpenEnter = func(c *Conn) {
		insideOnce.Do(func() { close(insideOpen) })
		<-releaseOpen
	}
	g.OnOpen(func(c *Conn) {
	})

	type dialResult struct {
		peer net.Conn
		nbc  *Conn
	}
	dialCh := make(chan dialResult, 1)
	go func() {
		peer, nbc := dialPeer(t, g)
		dialCh <- dialResult{peer: peer, nbc: nbc}
	}()
	waitChannel(t, insideOpen, time.Second, "OnOpen entered")
	var dr dialResult
	select {
	case dr = <-dialCh:
	case <-time.After(time.Second):
		t.Fatal("AddConn after OnOpen hung")
	}
	peer, nbc := dr.peer, dr.nbc
	select {
	case <-pendingSeen:
	case <-time.After(time.Second):
		t.Fatal("close pending never parked")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- nbc.Close() }()
	// The close request is parked until releaseOpen fires; the short yield only
	// lets that goroutine enter the Conn mutex. Synchrony is provided by the
	// engine's published/pending close state, not by scheduling timing.
	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)
	close(releaseOpen)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close during OnOpen: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close during OnOpen hung")
	}
	lc.waitCloseCount(t, 1, time.Second)

	// Repeated closes must be idempotent: the connection closes only once and
	// OnClose fires exactly once, with the locally supplied nil error.
	for i := 0; i < 3; i++ {
		if err := nbc.Close(); err != nil {
			t.Fatalf("redundant Close returned error: %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&lc.closeN); got != 1 {
		t.Fatalf("OnClose count = %d, want 1", got)
	}
	errs := lc.closeErrors()
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("OnClose errors = %v, want [nil]", errs)
	}
	closed, closeErr := nbc.IsClosed()
	if !closed {
		t.Fatal("conn should report closed")
	}
	if closeErr != nil {
		t.Fatalf("closeErr = %v, want nil", closeErr)
	}
	stopEngine(t, g)
	if err := peer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("peer close: %v", err)
	}
}

// TestCloseDuringOnOpenBarrier pins the exact interleaving: the racing Close
// runs while OnOpen is blocked on a barrier, then OnOpen returns and addConn
// finishes publishing the connection.
func TestCloseDuringOnOpenBarrier(t *testing.T) {
	g, lc := newLifecycleEngine(t, 1, mempool.NewSTD())
	h := installHooks(t)
	defer h.restore()

	var insideOnce sync.Once
	insideOpen := make(chan struct{})
	releaseOpen := make(chan struct{})
	closeDone := make(chan error, 1)

	g.OnOpen(func(c *Conn) {
		insideOnce.Do(func() { close(insideOpen) })
		<-releaseOpen
	})

	peer, nbc := dialPeerOffEngine(t, g)
	watchRemoteAddr(peer.LocalAddr().String())
	defer unwatchRemoteAddr(peer.LocalAddr().String())

	waitChannel(t, insideOpen, time.Second, "OnOpen reached barrier")
	go func() { closeDone <- nbc.Close() }()
	// Give the closer a fair chance to enter the Conn mutex while OnOpen is
	// still parked; the assertions rely on state, not this yield.
	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)
	close(releaseOpen)

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close during blocked OnOpen hung")
	}
	lc.waitCloseCount(t, 1, time.Second)

	stopEngine(t, g)
	if err := peer.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatalf("peer close: %v", err)
	}
}

// ===========================================================================
// Scenario 2: several buffers are queued in the write list when the peer shuts
// down. The queued buffers must be released once each, the OnWrittenSize bytes
// must never come from a released buffer, OnClose fires once with a peer error
// and the engine stops cleanly.
// ===========================================================================

func TestPeerCloseWithQueuedWriteBuffers(t *testing.T) {
	alloc := newTrackingAllocator()
	g, lc := newLifecycleEngine(t, 1, alloc)
	h := installHooks(t)
	defer h.restore()

	queued := make(chan struct{}, 1)
	releaseWriter := make(chan struct{})
	var queueOnce sync.Once
	hookWriteQueued = func(c *Conn) {
		queueOnce.Do(func() { close(queued) })
		<-releaseWriter
	}

	writerStarted := make(chan struct{})
	var nbc *Conn
	var startedOnce sync.Once
	g.OnOpen(func(c *Conn) {
		nbc = c
		watchRemoteAddr(c.RemoteAddr().String())
		startedOnce.Do(func() { close(writerStarted) })
	})

	peer, _ := dialPeer(t, g)
	defer func() { _ = peer.Close() }()
	waitChannel(t, writerStarted, time.Second, "server OnOpen")

	// Shrink the nbio-side send queue so a 128KiB write cannot drain in one
	// shot and definitely accumulates several toWrite buffers (>64KiB each is
	// a new buffer, see newToWriteBuf).
	if err := nbc.SetWriteBuffer(4096); err != nil {
		t.Fatalf("set send buffer: %v", err)
	}
	_ = nbc.SetNoDelay(false)

	const payloadSize = 128 * 1024
	writeDone := make(chan error, 1)
	go func() {
		_, err := nbc.Write(bytes.Repeat([]byte("Q"), payloadSize))
		writeDone <- err
	}()
	waitChannel(t, queued, time.Second, "multiple buffers queued")

	// Peer disappears while the writer is still parked at the deterministic
	// point: everything left in the queue must be torn down exactly once.
	if err := peer.Close(); err != nil {
		t.Fatalf("peer close: %v", err)
	}
	close(releaseWriter)

	select {
	case err := <-writeDone:
		if err != nil && !errors.Is(err, syscall.EPIPE) &&
			!errors.Is(err, syscall.ECONNRESET) && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Write returned unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write goroutine hung after peer close")
	}
	lc.waitCloseCount(t, 1, 2*time.Second)

	errs := lc.closeErrors()
	if len(errs) != 1 || !isPeerCloseError(errs[0]) {
		t.Fatalf("OnClose errors = %v, want a single peer-close error", errs)
	}

	// Every buffer consumed by onWrittenSize must belong to a still-live
	// allocation at the time it is reported.
	mallocN, freeN, _ := alloc.counts()
	if mallocN < 2 {
		t.Fatalf("expected multiple queued allocations, got malloc=%d", mallocN)
	}
	if freeN != mallocN {
		t.Fatalf("allocator unbalanced after close: malloc=%d free=%d", mallocN, freeN)
	}
	alloc.assertNoLiveReuse(t)

	stopEngine(t, g)
	alloc.assertBalanced(t)
}

// ===========================================================================
// Scenario 3: an OnData handler starts an asynchronous write and asks for
// close immediately afterwards. Both fixed interleavings are exercised:
//   - the write enters the Conn before the close (queued-then-close);
//   - the close wins the Conn mutex first (close-then-write).
// OnClose must still fire exactly once, queued buffers are released once each
// and no goroutine may stay parked inside the engine.
// ===========================================================================

// peerReader drains a peer connection until EOF, verifying that bytes echoed
// back by the server arrive in the original order with no corruption.
func peerReader(t *testing.T, peer net.Conn, want int64, gotAll chan<- struct{}) {
	t.Helper()
	buf := make([]byte, 4096)
	var got int64
	for {
		n, err := peer.Read(buf)
		if n > 0 {
			got += int64(n)
		}
		if err != nil {
			if got != want {
				t.Errorf("peer read got %d bytes, want %d: %v", got, want, err)
			}
			close(gotAll)
			return
		}
		if got >= want {
			close(gotAll)
			return
		}
	}
}

func writeThenCloseAsync(t *testing.T, closeFirst bool) {
	t.Helper()
	alloc := newTrackingAllocator()
	g, lc := newLifecycleEngine(t, 1, alloc)
	h := installHooks(t)
	defer h.restore()

	writerQueued := make(chan struct{}, 1)
	releaseWriter := make(chan struct{})
	var queueOnce sync.Once
	if !closeFirst {
		hookWriteQueued = func(c *Conn) {
			queueOnce.Do(func() { close(writerQueued) })
			<-releaseWriter
		}
	}

	dataArrived := make(chan struct{})
	releaseData := make(chan struct{})
	var nbc *Conn
	g.OnOpen(func(c *Conn) {
		nbc = c
		watchRemoteAddr(c.RemoteAddr().String())
	})
	g.OnData(func(c *Conn, data []byte) {
		close(dataArrived)
		<-releaseData
		payload := bytes.Repeat([]byte("W"), 128*1024)
		go func() {
			_, _ = c.Write(payload)
		}()
		if closeFirst {
			// Hand the write goroutine a chance to block waiting on c.mux,
			// then close first.
			runtime.Gosched()
			time.Sleep(20 * time.Millisecond)
		} else {
			// Wait until the async write has published several buffers, then
			// race the close with the parked writer.
			waitChannel(t, writerQueued, time.Second, "async write to queue buffers")
		}
		_ = c.Close()
		if !closeFirst {
			close(releaseWriter)
		}
	})

	// Small enough to be delivered through the loopback socket buffers
	// immediately; the queue-pressure test is scenario 2 instead.
	const payload = 4 * 1024
	peer, _ := dialPeer(t, g)
	defer func() { _ = peer.Close() }()
	waitChannel(t, func() chan struct{} {
		ch := make(chan struct{})
		go func() {
			_, _ = peer.Write(bytes.Repeat([]byte("D"), payload))
			close(ch)
		}()
		return ch
	}(), time.Second, "peer write")

	waitChannel(t, dataArrived, time.Second, "server OnData")
	// Force the async echo write to accumulate instead of draining.
	if nbc == nil {
		t.Fatal("OnOpen never ran")
	}
	if err := nbc.SetWriteBuffer(4096); err != nil {
		t.Fatalf("set write buffer: %v", err)
	}
	_ = nbc.SetNoDelay(false)
	close(releaseData)

	lc.waitCloseCount(t, 1, 2*time.Second)
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&lc.closeN); got != 1 {
		t.Fatalf("OnClose count = %d, want 1", got)
	}
	mallocN, freeN, _ := alloc.counts()
	if freeN != mallocN {
		t.Fatalf("allocator unbalanced: malloc=%d free=%d", mallocN, freeN)
	}
	alloc.assertNoLiveReuse(t)

	// Every further Close is a no-op and publishes nothing.
	for i := 0; i < 2; i++ {
		_ = nbc.Close()
	}
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt64(&lc.closeN); got != 1 {
		t.Fatalf("OnClose count after redundant closes = %d, want 1", got)
	}

	stopEngine(t, g)
	alloc.assertBalanced(t)
}

func TestAsyncWriteThenClose(t *testing.T) {
	t.Run("write queued before close", func(t *testing.T) {
		writeThenCloseAsync(t, false)
	})
	t.Run("close before queued write", func(t *testing.T) {
		writeThenCloseAsync(t, true)
	})
}

// ===========================================================================
// Scenario 5: the user OnClose callback closes the connection again.
// ===========================================================================

func TestCloseInsideOnClose(t *testing.T) {
	g, lc := newLifecycleEngine(t, 1, mempool.NewSTD())

	reentered := make(chan struct{}, 1)
	var nbc *Conn
	closeErrSentinel := errors.New("lifecycle test: close from user")
	g.OnClose(func(c *Conn, err error) {
		if err != closeErrSentinel {
			t.Errorf("OnClose err = %v, want %v", err, closeErrSentinel)
			return
		}
		// Redundant close from inside the close callback must be harmless and
		// must never synchronously re-enter this callback.
		if err := c.Close(); err != nil {
			t.Errorf("Close inside OnClose: %v", err)
			return
		}
		select {
		case reentered <- struct{}{}:
		default:
		}
	})

	g.OnOpen(func(c *Conn) {
		nbc = c
		watchRemoteAddr(c.RemoteAddr().String())
	})
	_, _ = dialPeer(t, g)
	waitUntil(t, func() bool { return nbc != nil }, time.Second, "OnOpen")
	if err := nbc.CloseWithError(closeErrSentinel); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitChannel(t, reentered, time.Second, "OnClose with nested Close")
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&lc.closeN); got != 1 {
		t.Fatalf("OnClose count = %d, want 1", got)
	}
	stopEngine(t, g)
}

func waitUntil(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timeout waiting for: %s", msg)
}
