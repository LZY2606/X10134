// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// testHooks holds test-only synchronization points used by the in-package
// lifecycle tests to pin down specific interleavings.
//
// The fields stay nil in production builds and every call site checks for
// nil first, so there is no public API surface and no behavior change when
// tests do not install hooks.
type testHooks struct {
	// beforeOnOpen blocks the OnOpen callback once it has been entered,
	// letting a test trigger a Close from another goroutine while the
	// callback is still in flight.
	beforeOnOpen func(c *Conn)

	// connAdded fires after a connection has completed registration with
	// its poller (callback fired and fd tracked).
	connAdded func(c *Conn)

	// stopAfterListeners fires from Engine.Stop right after every listener
	// has been closed and before live connections are snapshotted.
	stopAfterListeners func()

	// inWrite fires from the unix Conn.Write with the conn mutex held,
	// before the closed flag is consulted. It lets a test park a foreign
	// writer at an exact point and then race Close against it.
	inWrite func(c *Conn)

	// closing fires from the unix closeWithError before the conn mutex is
	// acquired, giving tests a deterministic rendezvous with the close path.
	closing func(c *Conn)
}

func (g *Engine) hookBeforeOnOpen(c *Conn) {
	if h := g.testHooks; h != nil && h.beforeOnOpen != nil {
		h.beforeOnOpen(c)
	}
}

func (g *Engine) hookConnAdded(c *Conn) {
	if h := g.testHooks; h != nil && h.connAdded != nil {
		h.connAdded(c)
	}
}

func (g *Engine) hookStopAfterListeners() {
	if h := g.testHooks; h != nil && h.stopAfterListeners != nil {
		h.stopAfterListeners()
	}
}
