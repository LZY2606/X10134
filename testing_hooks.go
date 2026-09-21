// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only lifecycle synchronization points.
//
// These variables are nil in production builds (their only call sites are
// nil-checked), and are only assigned from *_test.go files. They let the
// lifecycle tests drive the engine/poller goroutines through exact phases
// of the accept / write / close / stop interleavings without relying on
// scheduler timing. They are intentionally unexported: no new public API
// is introduced.
var (
	// testHookAcceptConn runs on the listener goroutine after Accept returns
	// a connection and before it is published to an IO poller.
	testHookAcceptConn func()

	// testHookStopSnapshot runs after Engine.Stop has closed the listeners
	// and right before the connection tables are snapshotted under g.mux.
	testHookStopSnapshot func()

	// testHookWriteEnter runs at the beginning of Conn.Write/Writev,
	// before the conn mutex is taken.
	testHookWriteEnter func()

	// testHookWriteLocked runs inside Write after the conn is confirmed open.
	// On *nix it is invoked while the conn mutex is held; on the std backend
	// it pins the writer before the underlying write syscall.
	testHookWriteLocked func()
)
