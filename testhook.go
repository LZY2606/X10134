// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Synchronization hooks used exclusively by the package's tests to
// reproduce deterministic lifecycle interleavings. They are unexported,
// nil in production, and must only be set before Engine.Start and reset
// after Engine.Stop has returned, so that they never race with engine
// goroutines.
var (
	// testHookAfterAccept is called by a listener after a successful
	// Accept and before the new connection is registered to a poller.
	testHookAfterAccept func()

	// testHookStopEntered is called at the beginning of Engine.Stop.
	testHookStopEntered func()
)
