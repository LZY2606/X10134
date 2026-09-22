// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import "syscall"

//go:norace
func connID(c *Conn) int { return c.fd }

//go:norace
func isPeerClosedSyscallErr(err error) bool {
	switch err {
	case syscall.ECONNRESET, syscall.EPIPE, syscall.ECONNABORTED, syscall.ENOTCONN:
		return true
	}
	return false
}
