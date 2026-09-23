// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only synchronization hooks.
//
// These hooks are nil in production and are only set by tests in this
// package to force deterministic interleavings between the Engine, the
// pollers and user callbacks. They are unexported, are not part of the
// public API, and must never be relied on by user code.
var (
	// testHookAfterAccept is called by a listener poller after a new
	// connection has been accepted and converted, but before it is
	// registered to an IO poller.
	testHookAfterAccept func()

	// testHookAfterListenersStopped is called by Engine.Stop after all
	// listeners have been stopped and before the connections snapshot
	// is taken.
	testHookAfterListenersStopped func()

	// testHookAfterConnsSnapshot is called by Engine.Stop after the
	// connections snapshot has been taken and before the snapshot
	// connections are closed.
	testHookAfterConnsSnapshot func()
)
