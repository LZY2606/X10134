// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build darwin || netbsd || freebsd || openbsd || dragonfly
// +build darwin netbsd freebsd openbsd dragonfly

package nbio

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/lesismal/nbio/logging"
)

const (
	// EPOLLLT .
	EPOLLLT = 0

	// EPOLLET .
	EPOLLET = 1

	// EPOLLONESHOT .
	EPOLLONESHOT = 0
)

const (
	IPPROTO_TCP   = 0
	TCP_KEEPINTVL = 0
	TCP_KEEPIDLE  = 0
)

// wakeupToken is written to the IO poller wakeup socketpair to unblock its
// Kevent wait so it can pick up newly registered changes.
var wakeupToken = []byte{1}

type poller struct {
	mux sync.Mutex

	g *Engine

	kfd    int
	evtfd  int
	wakefd int
	// stopfd is the read end of a dedicated shutdown socketpair. It is never
	// read, so once stop() writes a byte it stays permanently readable and the
	// IO loop exits regardless of scheduling.
	stopfd int
	stopw  int

	index int

	shutdown bool

	listener     net.Listener
	isListener   bool
	unixSockAddr string

	ReadBuffer []byte

	pollType string

	eventList []syscall.Kevent_t
}

//go:norace
func (p *poller) addConn(c *Conn) error {
	fd := c.fd
	if fd >= len(p.g.connsUnix) {
		err := fmt.Errorf("too many open files, fd[%d] >= MaxOpenFiles[%d]",
			fd,
			len(p.g.connsUnix))
		_ = c.closeWithError(err)
		return err
	}
	c.p = p
	if c.typ != ConnTypeUDPServer {
		p.g.onOpen(c)
	} else {
		p.g.onUDPListen(c)
	}
	p.g.connsUnix[fd] = c
	p.addRead(fd)
	return nil
}

//go:norace
func (p *poller) addDialer(c *Conn) error {
	fd := c.fd
	if fd >= len(p.g.connsUnix) {
		err := fmt.Errorf("too many open files, fd[%d] >= MaxOpenFiles[%d]",
			fd,
			len(p.g.connsUnix),
		)
		_ = c.closeWithError(err)
		return err
	}
	c.p = p
	p.g.connsUnix[fd] = c
	c.isWAdded = true
	p.addReadWrite(fd)
	return nil
}

//go:norace
func (p *poller) getConn(fd int) *Conn {
	return p.g.connsUnix[fd]
}

//go:norace
func (p *poller) deleteConn(c *Conn) {
	if c == nil {
		return
	}
	fd := c.fd

	if c.typ != ConnTypeUDPClientFromRead {
		if c == p.g.connsUnix[fd] {
			p.g.connsUnix[fd] = nil
		}
		// p.deleteEvent(fd)
	}

	if c.typ != ConnTypeUDPServer {
		p.g.onClose(c, c.closeErr)
	}
}

//go:norace
func (p *poller) trigger() error {
	_, err := syscall.Write(p.evtfd, wakeupToken)
	if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
		// Pipe already holds pending tokens; the wakeup is still pending.
		return nil
	}
	return err
}

//go:norace
func (p *poller) addRead(fd int) {
	p.mux.Lock()
	p.eventList = append(p.eventList, syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_ADD, Filter: syscall.EVFILT_READ})
	// p.eventList = append(p.eventList, syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_ADD, Filter: syscall.EVFILT_WRITE})
	p.mux.Unlock()
	p.trigger()
}

//go:norace
func (p *poller) resetRead(fd int) error {
	p.mux.Lock()
	p.eventList = append(p.eventList, syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_DELETE, Filter: syscall.EVFILT_WRITE})
	p.mux.Unlock()
	return p.trigger()
}

//go:norace
func (p *poller) modWrite(fd int) error {
	p.mux.Lock()
	p.eventList = append(p.eventList, syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_ADD, Filter: syscall.EVFILT_WRITE})
	p.mux.Unlock()
	return p.trigger()
}

//go:norace
func (p *poller) addReadWrite(fd int) {
	p.mux.Lock()
	p.eventList = append(p.eventList, syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_ADD, Filter: syscall.EVFILT_READ})
	p.eventList = append(p.eventList, syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_ADD, Filter: syscall.EVFILT_WRITE})
	p.mux.Unlock()
	p.trigger()
}

// func (p *poller) deleteEvent(fd int) {
// 	p.mux.Lock()
// 	p.eventList = append(p.eventList,
// 		syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_DELETE, Filter: syscall.EVFILT_READ},
// 		syscall.Kevent_t{Ident: uint64(fd), Flags: syscall.EV_DELETE, Filter: syscall.EVFILT_WRITE})
// 	p.mux.Unlock()
// 	p.trigger()
// }

//go:norace
func (p *poller) readWrite(ev *syscall.Kevent_t) {
	if ev.Flags&syscall.EV_DELETE > 0 {
		return
	}
	fd := int(ev.Ident)
	c := p.getConn(fd)
	if c != nil {
		if ev.Filter == syscall.EVFILT_READ {
			if p.g.onRead == nil {
				for {
					pbuf := p.g.borrow(c)
					bufLen := len(*pbuf)
					rc, n, err := c.ReadAndGetConn(pbuf)
					if n > 0 {
						*pbuf = (*pbuf)[:n]
						p.g.onDataPtr(rc, pbuf)
					}
					p.g.payback(c, pbuf)
					if errors.Is(err, syscall.EINTR) {
						continue
					}
					if errors.Is(err, syscall.EAGAIN) {
						return
					}
					if (err != nil || n == 0) && ev.Flags&syscall.EV_DELETE == 0 {
						if err == nil {
							err = io.EOF
						}
						_ = c.closeWithError(err)
					}
					if n < bufLen {
						break
					}
				}
			} else {
				p.g.onRead(c)
			}

			if ev.Flags&syscall.EV_EOF != 0 {
				if c.onConnected == nil {
					_ = c.flush()
				} else {
					c.onConnected(c, nil)
					c.onConnected = nil
					c.resetRead()
				}
			}
		}

		if ev.Filter == syscall.EVFILT_WRITE {
			if c.onConnected == nil {
				_ = c.flush()
			} else {
				c.resetRead()
				c.onConnected(c, nil)
				c.onConnected = nil
			}
		}
	}
}

//go:norace
func (p *poller) start() {
	if p.g.LockPoller {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}
	defer p.g.Done()

	logging.Debug("NBIO[%v][%v_%v] start", p.g.Name, p.pollType, p.index)
	defer logging.Debug("NBIO[%v][%v_%v] stopped", p.g.Name, p.pollType, p.index)

	if p.isListener {
		p.acceptorLoop()
	} else {
		defer func() {
			_ = syscall.Close(p.kfd)
			_ = syscall.Close(p.evtfd)
			_ = syscall.Close(p.wakefd)
			_ = syscall.Close(p.stopfd)
			_ = syscall.Close(p.stopw)
		}()
		p.readWriteLoop()
	}
}

//go:norace
func (p *poller) acceptorLoop() {
	if p.g.LockListener {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	p.shutdown = false
	for !p.shutdown {
		conn, err := p.listener.Accept()
		if err == nil {
			var c *Conn
			c, err = NBConn(conn)
			if err != nil {
				_ = conn.Close()
				continue
			}
			if testHookAfterAccepted != nil {
				testHookAfterAccepted(c)
			}
			_ = p.g.pollers[c.Hash()%len(p.g.pollers)].addConn(c)
		} else {
			var ne net.Error
			if ok := errors.As(err, &ne); ok && ne.Timeout() {
				logging.Error("NBIO[%v][%v_%v] Accept failed: timeout error, retrying...", p.g.Name, p.pollType, p.index)
				time.Sleep(time.Second / 20)
			} else {
				if !p.shutdown {
					logging.Error("NBIO[%v][%v_%v] Accept failed: %v, exit...", p.g.Name, p.pollType, p.index, err)
				}
				if p.g.onAcceptError != nil {
					p.g.onAcceptError(err)
				}
			}
		}
	}
}

//go:norace
func (p *poller) readWriteLoop() {
	if p.g.LockPoller {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
	}

	events := make([]syscall.Kevent_t, 1024)
	var changes []syscall.Kevent_t

	p.shutdown = false
	drainBuf := make([]byte, 256)
	for !p.shutdown {
		p.mux.Lock()
		changes = p.eventList
		p.eventList = nil
		p.mux.Unlock()

		// Indefinite wait: shutdown is delivered as data on the wakeup
		// socketpair (shutdownToken), which cannot be lost across scheduling.
		n, err := syscall.Kevent(p.kfd, changes, events, nil)
		if err != nil && !errors.Is(err, syscall.EINTR) && !errors.Is(err, syscall.EBADF) && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.EINVAL) {
			logging.Error("NBIO[%v][%v_%v] Kevent failed: %v, exit...", p.g.Name, p.pollType, p.index, err)
			p.shutdown = true
			return
		}

		for i := 0; i < n; i++ {
			if int(events[i].Ident) == p.stopfd || int(events[i].Ident) == p.wakefd {
				logging.Error("DBG[%v] event ident=%v stopfd=%v wakefd=%v flags=%v", p.index, events[i].Ident, p.stopfd, p.wakefd, events[i].Flags)
			}
			switch int(events[i].Ident) {
			case p.stopfd:
				// Dedicated shutdown signal: never drained, stays readable.
				p.shutdown = true
			case p.wakefd:
				// Drain ordinary wakeup tokens so the level resets.
				for {
					rn, rerr := syscall.Read(p.wakefd, drainBuf)
					if rerr != nil || rn == 0 {
						break
					}
				}
			default:
				p.readWrite(&events[i])
			}
		}
	}
}

//go:norace
func (p *poller) stop() {
	logging.Debug("NBIO[%v][%v_%v] stop...", p.g.Name, p.pollType, p.index)
	if p.listener != nil {
		p.shutdown = true
		_ = p.listener.Close()
		if p.unixSockAddr != "" {
			_ = os.Remove(p.unixSockAddr)
		}
		return
	}
	p.shutdown = true
	// Signal shutdown through the dedicated (never-drained) pipe. One byte
	// makes its read end permanently readable, waking a parked Kevent or
	// being reported immediately on the next wait; EAGAIN just means it is
	// already readable from a previous call.
	nw, werr := syscall.Write(p.stopw, wakeupToken)
	logging.Error("DBG stop[%v] wrote n=%v err=%v stopw=%v stopfd=%v", p.index, nw, werr, p.stopw, p.stopfd)
}

//go:norace
func newPoller(g *Engine, isListener bool, index int) (*poller, error) {
	if isListener {
		if len(g.Addrs) == 0 {
			panic("invalid listener num")
		}

		addr := g.Addrs[index%len(g.Addrs)]
		ln, err := g.Listen(g.Network, addr)
		if err != nil {
			return nil, err
		}

		p := &poller{
			g:          g,
			index:      index,
			listener:   ln,
			isListener: isListener,
			pollType:   "LISTENER",
		}
		if g.Network == "unix" {
			p.unixSockAddr = addr
		}

		return p, nil
	}

	fd, err := syscall.Kqueue()
	if err != nil {
		return nil, err
	}

	// Level-triggered wakeup socketpair (read end registered with kqueue),
	// mirroring the eventfd used on linux. Unlike EVFILT_USER, a token written
	// before the loop parks is never coalesced away by a concurrent Kevent
	// changelist call.
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	rfd, wfd := fds[0], fds[1]
	if err := syscall.SetNonblock(rfd, true); err != nil {
		_ = syscall.Close(fd)
		_ = syscall.Close(rfd)
		_ = syscall.Close(wfd)
		return nil, err
	}
	if err := syscall.SetNonblock(wfd, true); err != nil {
		_ = syscall.Close(fd)
		_ = syscall.Close(rfd)
		_ = syscall.Close(wfd)
		return nil, err
	}

	_, err = syscall.Kevent(fd, []syscall.Kevent_t{{
		Ident:  uint64(rfd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}}, nil, nil)
	if err != nil {
		_ = syscall.Close(fd)
		_ = syscall.Close(rfd)
		_ = syscall.Close(wfd)
		return nil, err
	}

	// Dedicated shutdown pipe: distinct from the ordinary wakeup pipe so a
	// shutdown signal can never be consumed/drained together with normal
	// change-list wakeups.
	sfds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		_ = syscall.Close(fd)
		_ = syscall.Close(rfd)
		_ = syscall.Close(wfd)
		return nil, err
	}
	srfd, swfd := sfds[0], sfds[1]
	cleanupStop := func() {
		_ = syscall.Close(srfd)
		_ = syscall.Close(swfd)
	}
	if err := syscall.SetNonblock(srfd, true); err != nil {
		_ = syscall.Close(fd)
		_ = syscall.Close(rfd)
		_ = syscall.Close(wfd)
		cleanupStop()
		return nil, err
	}
	if err := syscall.SetNonblock(swfd, true); err != nil {
		_ = syscall.Close(fd)
		_ = syscall.Close(rfd)
		_ = syscall.Close(wfd)
		cleanupStop()
		return nil, err
	}
	_, err = syscall.Kevent(fd, []syscall.Kevent_t{{
		Ident:  uint64(srfd),
		Filter: syscall.EVFILT_READ,
		Flags:  syscall.EV_ADD,
	}}, nil, nil)
	if err != nil {
		_ = syscall.Close(fd)
		_ = syscall.Close(rfd)
		_ = syscall.Close(wfd)
		cleanupStop()
		return nil, err
	}

	p := &poller{
		g:          g,
		kfd:        fd,
		evtfd:      wfd,
		wakefd:     rfd,
		stopfd:     srfd,
		stopw:      swfd,
		index:      index,
		isListener: isListener,
		pollType:   "POLLER",
	}

	return p, nil
}

//go:norace
func (c *Conn) ResetPollerEvent() {
}
