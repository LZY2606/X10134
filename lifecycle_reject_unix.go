//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import "syscall"

// rejectAcceptedConnDuringStop closes a connection whose accept-side addConn
// raced with Engine.Stop. The connection was never published nor counted in
// the engine's wgConn (onOpen is invoked after publication), so only the raw fd
// must be released.
//
//go:norace
func rejectAcceptedConnDuringStop(c *Conn) {
	_ = syscall.Close(c.fd)
}

// rejectDialerConnDuringStop closes an in-flight dialer whose addDialer raced
// with Engine.Stop. DialAsync already did wgConn.Add(1) before addDialer and
// the connection is never published nor gets an onClose, so wgConn must be
// balanced here.
//
//go:norace
func rejectDialerConnDuringStop(c *Conn) {
	c.p.g.wgConn.Done()
	_ = syscall.Close(c.fd)
}
