// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Synchronization hooks used by the package tests to pin down specific
// interleavings. They stay nil in production code and are only set from
// *_test.go files, so no public API is introduced.
var (
	// testHookAfterAccept is invoked on the acceptor goroutine right after a
	// connection has been accepted and before it is handed to an IO poller.
	testHookAfterAccept func()
)
