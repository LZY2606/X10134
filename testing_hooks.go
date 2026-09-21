// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only synchronization points.
//
// These hooks are nil in production builds and stay unexported, so no new
// public API is introduced. White-box tests in this package may assign them
// to drive deterministic interleavings instead of relying on sleeps or on
// incidental scheduler behavior.
var (
	// testHookAfterAccepted runs on the listener goroutine after Accept has
	// returned a connection and before the connection is added to an IO
	// poller (which is where onOpen is invoked).
	testHookAfterAccepted func(c *Conn)

	// testHookAfterListenersClosed runs on the caller of Engine.Stop after
	// every listener has been asked to stop (Accept unblocked) but before the
	// listener goroutines are joined and live connections snapshotted.
	testHookAfterListenersClosed func()
)
