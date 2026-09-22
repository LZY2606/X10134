//go:build !windows
// +build !windows

package nbio

import (
	"errors"
	"net"
	"runtime"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// backlogConnForStopRace returns a fully accepted loopback TCP Conn that is
// sitting in a throwaway raw listener's accept backlog: the peer side has
// connected, but Accept has not been called yet. The raw listener is closed
// when the engine rejects the connection during Stop; the caller closes it
// after the test.
func backlogConnForStopRace(g *Engine) (*Conn, net.Listener, error) {
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	tln := ln.(*net.TCPListener)
	rc, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, syscall.IPPROTO_TCP)
	if err != nil {
		_ = ln.Close()
		return nil, nil, err
	}
	if err := syscall.SetNonblock(rc, true); err != nil {
		_ = syscall.Close(rc)
		_ = ln.Close()
		return nil, nil, err
	}
	sa, err := sockaddrInet4(tln.Addr().(*net.TCPAddr))
	if err != nil {
		_ = syscall.Close(rc)
		_ = ln.Close()
		return nil, nil, err
	}
	if err := syscall.Connect(rc, sa); err != nil && !errors.Is(err, syscall.EINPROGRESS) {
		_ = syscall.Close(rc)
		_ = ln.Close()
		return nil, nil, err
	}
	c := &Conn{fd: rc, typ: ConnTypeTCP, lAddr: tln.Addr(), rAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)}}
	return c, ln, nil
}

func sockaddrInet4(taddr *net.TCPAddr) (syscall.Sockaddr, error) {
	ip := taddr.IP.To4()
	if ip == nil {
		ip = net.IPv4(127, 0, 0, 1).To4()
	}
	return &syscall.SockaddrInet4{Addr: [4]byte{ip[0], ip[1], ip[2], ip[3]}, Port: taddr.Port}, nil
}

// ===========================================================================
// Scenario 4: a new connection finishes accept/addConn while Engine.Stop is
// closing connections. The connection must be closed too, OnClose must still
// fire exactly once and Stop must not hang waiting for wgConn.
// ===========================================================================

func TestAcceptDuringStop(t *testing.T) {
	alloc := newTrackingAllocator()
	g, lc := newLifecycleEngine(t, 2, alloc)
	h := installHooks(t)
	defer h.restore()

	sentinelOpen := make(chan struct{})
	sentinelRelease := make(chan struct{})
	g.OnOpen(func(c *Conn) {
		// Only the first accepted connection participates in the barrier.
		select {
		case <-sentinelOpen:
		default:
			close(sentinelOpen)
			<-sentinelRelease
		}
	})

	sentinel, err := Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatalf("dial sentinel: %v", err)
	}
	sentinel, err = g.AddConn(sentinel)
	if err != nil {
		t.Fatalf("add sentinel conn: %v", err)
	}
	var sentinelConn *Conn = sentinel
	watchRemoteAddr(sentinel.RemoteAddr().String())
	defer unwatchRemoteAddr(sentinel.RemoteAddr().String())
	waitChannel(t, sentinelOpen, time.Second, "sentinel accepted and blocked in OnOpen")

	// Freeze Stop's connection sweep at the sentinel, which is deliberately
	// accepted first, so the "late" connection lands in the shutdown window.
	iterReached := make(chan struct{}, 1)
	releaseIter := make(chan struct{})
	hookStopConnIter = func(c *Conn) {
		if c == sentinelConn {
			select {
			case iterReached <- struct{}{}:
			default:
			}
			<-releaseIter
		}
	}

	stopReturned := make(chan struct{})
	go func() {
		g.Stop()
		close(stopReturned)
	}()
	waitChannel(t, iterReached, time.Second, "Stop sweep reaches sentinel")

	// Listener is already closed: take an accepted connection that sits in a
	// raw listener backlog and run it through addConn while the sweep is
	// frozen, which makes the accept/stop interleaving deterministic without
	// racing the listener shutdown itself.
	late, raw, err := backlogConnForStopRace(g)
	if err != nil {
		t.Fatalf("create racy conn: %v", err)
	}
	defer func() { _ = raw.Close() }()
	targetPoller := g.pollers[late.Hash()%len(g.pollers)]
	addErr := make(chan error, 1)
	go func() { addErr <- targetPoller.addConn(late) }()

	// Whether addConn lands just before or after the stopping publication it
	// must either be swept by the in-flight Stop or rejected with a closed fd
	// and a balanced wgConn. Give it a deterministic beat, then release the
	// sweep and require Stop to come back promptly either way.
	runtime.Gosched()
	time.Sleep(20 * time.Millisecond)
	close(releaseIter)
	select {
	case err := <-addErr:
		if err != nil {
			t.Fatalf("addConn during stop: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("addConn during stop hung")
	}

	waitChannel(t, stopReturned, 5*time.Second, "Stop with racing accept")
	lc.waitCloseCount(t, 2, 2*time.Second)
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt64(&lc.closeN); got != 2 {
		t.Fatalf("OnClose count = %d, want 2", got)
	}
	alloc.assertBalanced(t)
}
