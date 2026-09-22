// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// Test-only synchronization hooks.
//
// These variables are no-op in production and are only reassigned from
// *_test.go files. They exist so lifecycle tests can pin down exact
// interleavings (e.g. block a poller at a specific phase) without
// relying on scheduler timing or sleeps. No exported API is added; user
// code must never touch these.
var (
	// testHookPreRegister is invoked on the accepting goroutine after
	// onOpen returns but before the conn is published into the engine's
	// conn table and the read event is armed.
	testHookPreRegister func(c *Conn)

	// testHookWriteLocked is invoked from Write/Writev while holding
	// c.mux, right after the closed check passes.
	testHookWriteLocked func(c *Conn)

	// testHookListenerStopped is invoked by a listener poller when it
	// has been asked to stop and the underlying listener is closed.
	testHookListenerStopped func(index int)
)
