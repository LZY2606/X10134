// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only synchronization hooks.
//
// These variables are nil in production builds, they are only assigned
// from the lifecycle tests (same package) to pin specific goroutine
// interleavings. No exported API is added.

var (
	// hookAfterAccept runs after a listener Accept returns, before the
	// accepted conn is converted/registered.
	hookAfterAccept func(raw interface{})

	// hookStopAfterConns runs right after the conns slice/map has been
	// snapshotted in Engine.Stop, while stopping is already marked.
	hookStopAfterConns func()

	// hookPollerStop runs for every io poller stop in Engine.Stop.
	hookPollerStop func(index int, isListener bool)
)

//go:norace
func runHookAfterAccept(raw interface{}) {
	if hookAfterAccept != nil {
		hookAfterAccept(raw)
	}
}

//go:norace
func runHookStopAfterConns() {
	if hookStopAfterConns != nil {
		hookStopAfterConns()
	}
}

//go:norace
func runHookPollerStop(index int, isListener bool) {
	if hookPollerStop != nil {
		hookPollerStop(index, isListener)
	}
}
