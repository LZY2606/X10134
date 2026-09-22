// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Synchronization hooks used only by in-package lifecycle tests to pin down
// specific interleavings without relying on scheduler timing.
//
// All hooks default to nil, so non-test builds pay one nil comparison per
// call site at most. They never appear in the public API and are always
// restored by tests.
var (
	// hookConnBeforeWrite is invoked by Conn.Write right before it takes
	// the connection lock.
	hookConnBeforeWrite func(*Conn)

	// hookConnBeforeClose is invoked at the very beginning of the close
	// path, before any state transition.
	hookConnBeforeClose func(*Conn)

	// hookEngineStopBegin is invoked by Engine.Stop right after the
	// listeners have been stopped but before Stop waits for the listener
	// goroutines to finish.
	hookEngineStopBegin func(*Engine)

	// hookListenerAccepted is invoked on the listener goroutine right after
	// a connection has been accepted and converted, before it is registered
	// with an IO poller.
	hookListenerAccepted func(c *Conn)
)
