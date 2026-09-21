// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

// writeListLen returns the number of pending write fragments. Called only at
// deterministic barriers where the poller cannot concurrently flush (peer not
// reading); reads the slice header under the conn lock.
func writeListLen(c *Conn) int {
	c.mux.Lock()
	n := len(c.writeList)
	c.mux.Unlock()
	return n
}

// writeQueueSupported reports whether the platform caches unsent writes in a
// per-conn write list driven by write-ready events.
const writeQueueSupported = true
