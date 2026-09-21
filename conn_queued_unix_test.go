// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

import "testing"

func queuedLen(_ *testing.T, c *Conn) int {
	c.mux.Lock()
	n := len(c.writeList)
	c.mux.Unlock()
	return n
}
