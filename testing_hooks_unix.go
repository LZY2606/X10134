// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

// Unix-only test synchronization points, see testing_hooks.go.
var (
	// testHookFlushWritten runs on the poller goroutine from flush after a
	// successful write to the head buffer, before that head is released.
	testHookFlushWritten func(c *Conn)

	// testHookReleaseWrite runs with the conn mutex held, just before the
	// head write buffer is released after a successful flush write.
	testHookReleaseWrite func(c *Conn)
)

// resetTestHooksPlatform clears unix-only test hooks.
func resetTestHooksPlatform() {
	testHookFlushWritten = nil
	testHookReleaseWrite = nil
}
