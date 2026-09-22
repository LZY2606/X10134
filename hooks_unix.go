// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

// Test-only synchronization points.
//
// These hooks stay nil in production builds and cost one nil branch per
// call site. They let the lifecycle tests pin otherwise racy interleavings
// to a concrete stage instead of relying on random scheduling. No hook is
// part of the public API and tests must reset every hook after use.
var (
	// testHookConnAfterOpen is invoked after a conn's onOpen callback has
	// returned, while the conn is still marked opening.
	testHookConnAfterOpen func(c *Conn)

	// testHookConnWrite is invoked by Conn.Write with c.mux released, right
	// after the closed check; Write re-checks closed when the hook returns.
	testHookConnWrite func(c *Conn)

	// testHookConnFlush is invoked by the poller's flush with c.mux held
	// and writeList non-empty, before any syscall.Write.
	testHookConnFlush func(c *Conn)

	// testHookStopAfterStoppingSet is invoked by Stop right after the
	// stopping flag is set, with g.mux held.
	testHookStopAfterStoppingSet func()
)
