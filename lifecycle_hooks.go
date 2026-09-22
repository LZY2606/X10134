// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only synchronization points.
//
// These hooks are always nil in production code and are only assigned from
// _test.go files inside this package. They do not extend the public API and
// exist solely to let lifecycle tests drive fixed interleavings instead of
// relying on random scheduling or sleeps.
var (
	// testHookConnWriteEnter is invoked at the entry of (*Conn).Write and
	// (*Conn).Writev, before any state is touched.
	testHookConnWriteEnter func(c *Conn)

	// testHookStopListenersDone is invoked from Engine.Stop right after the
	// listeners have been stopped but before the conns map is snapshotted.
	testHookStopListenersDone func()
)
