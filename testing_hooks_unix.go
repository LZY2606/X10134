// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

// Test-only synchronization hooks for the *nix poller/write paths.
// Unexported no-ops, not part of any public API.
// testHookConnFlush is invoked on the poller goroutine at the start of
// Conn.flush after queued data has been observed, before the conn mutex
// is taken. It lets tests park the flush and stack writes.
var testHookConnFlush = func(c *Conn) {}
