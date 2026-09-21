// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only synchronization hooks.
//
// These hooks exist to let the in-package lifecycle tests pin the engine,
// poller and connection state machines to specific interleavings without
// relying on scheduler timing or sleeps. They are unexported, default to
// no-ops and are not part of any public API; production builds pay one
// indirect call on the hooked paths.
var (
	// testHookAcceptConn is invoked on the listener goroutine right after
	// a connection has been accepted and converted, before it is
	// registered on an IO poller.
	testHookAcceptConn = func(c *Conn) {}

	// testHookStopListeners is invoked from Stop right after the listeners
	// have been asked to stop and before in-flight addConn registrations
	// are drained.
	testHookStopListeners = func() {}

	// testHookAddConnBegin is invoked at the very beginning of an IO
	// poller's addConn.
	testHookAddConnBegin = func(c *Conn) {}

	// testHookConnFlushed is invoked right after a successful write of n
	// bytes of a cached write buffer, before the buffer is released.
	testHookConnFlushed = func(c *Conn, b []byte, n int) {}

	// testHookWriteBufReleased is invoked after a fully-written cached
	// buffer has been returned to the allocator. The write callback must
	// have completed before this fires.
	testHookWriteBufReleased = func(c *Conn, b []byte) {}
)
