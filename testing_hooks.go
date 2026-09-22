// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only synchronization hooks.
//
// These are unexported package variables defaulting to no-op functions, so
// they add no observable behavior to production binaries and do not introduce
// any new public API. White-box tests in *_test.go may replace them to pin
// specific lifecycle interleavings (for example to continue an Engine.Stop
// call only after the listener goroutines have been joined).
var (
	// testHookStopAcceptorsJoined is invoked by Engine.Stop after every
	// listener has been closed and the listener goroutines joined, before the
	// connection maps are snapshotted.
	testHookStopAcceptorsJoined = func() {}

	// testHookStopConnsSnapshot is invoked by Engine.Stop right after the
	// *nix connection slice snapshot has been taken.
	testHookStopConnsSnapshot = func(connsUnix []*Conn) {}

	// testHookConnRegistered is invoked by a poller immediately after a newly
	// accepted connection has been registered with it.
	testHookConnRegistered = func(c *Conn) {}
)
