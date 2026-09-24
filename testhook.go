// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// testHooks holds synchronization hooks that are only set by in-package
// tests to reproduce deterministic lifecycle interleavings. Every hook is
// nil in production and tests must reset them to nil when done. Hooks are
// unexported, so they don't extend the public API.
var testHooks struct {
	// afterAccept is called by a listener poller after a connection has
	// been accepted and converted to *Conn, before it is added to a
	// poller.
	afterAccept func()
	// afterAddConn is called by a listener poller after an accepted
	// connection has been added to a poller.
	afterAddConn func()
	// stopBeforeConnSweep is called by Engine.Stop after the listeners
	// have been stopped, before the connections are snapshot and closed.
	stopBeforeConnSweep func()
}
