// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build windows
// +build windows

package nbio

//go:norace
func connID(c *Conn) int { return c.hash }

//go:norace
func isPeerClosedSyscallErr(err error) bool {
	if err == nil {
		return false
	}
	// Windows surfaces resets through WSAECONNRESET/EPIPE text; the
	// portable message-based check in isPeerClosedError covers them.
	return false
}
