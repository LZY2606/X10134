// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// connTarget publishes a *Conn identity safely between the test goroutine
// and the goroutine running OnOpen (for client-side AddConn these are the
// same goroutine; for accepted connections they differ).
type connTarget struct {
	mu sync.Mutex
	c  *Conn
}

func (t *connTarget) set(c *Conn) {
	t.mu.Lock()
	t.c = c
	t.mu.Unlock()
}

func (t *connTarget) is(c *Conn) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.c == c
}

// Scenario 1: OnOpen has not returned yet (it is still running on a nbio
// goroutine) when another goroutine requests Close.
//
// Contract pinned:
//   - OnOpen completes exactly once;
//   - OnClose fires exactly once for the connection;
//   - Close is effective immediately: a Write after it returns fails with
//     net.ErrClosed, and re-entrant Close calls are no-ops;
//   - the engine still shuts down cleanly with no goroutine leak.
func TestLifecycleCloseDuringOnOpen(t *testing.T) {
	var (
		cc              = newConnCounters()
		allowOpenReturn = make(chan struct{})
		openEntered     = make(chan struct{})
		target          connTarget
	)

	g := newLifecycleEngine(t, 2,
		func(c *Conn) {
			if !target.is(c) {
				return
			}
			cc.onOpen(c)
			close(openEntered)
			<-allowOpenReturn
		},
		cc.onData,
		cc.onClose,
		nil,
	)

	rawConn, err := net.Dial("tcp", g.Addrs[0])
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = rawConn.Close() }()
	nbc, err := NBConn(rawConn)
	if err != nil {
		t.Fatalf("NBConn: %v", err)
	}
	target.set(nbc)

	// Run AddConn in a goroutine: for a client-side connection OnOpen runs
	// inline, and we need another goroutine to interact while it blocks.
	addDone := make(chan struct{})
	go func() {
		if _, err := g.AddConn(nbc); err != nil {
			t.Errorf("addconn: %v", err)
		}
		close(addDone)
	}()

	select {
	case <-openEntered:
	case <-time.After(5 * time.Second):
		t.Fatalf("OnOpen never entered")
	}

	// OnOpen is blocked; request close from the test goroutine.
	if err := nbc.Close(); err != nil {
		t.Fatalf("Close error: %v", err)
	}

	// Close must be effective immediately for subsequent writes even though
	// OnOpen has not returned yet.
	if _, err := nbc.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Write after Close err = %v, want net.ErrClosed", err)
	}

	// Whether OnClose lands before or after OnOpen returns is an
	// implementation detail; it must have fired exactly once shortly after
	// the connection is allowed to complete opening.
	close(allowOpenReturn)

	select {
	case <-addDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("AddConn did not return after OnOpen completed")
	}

	waitFor(t, "OnOpen/OnClose counts", 5*time.Second, func() bool {
		return cc.opens() == 1 && cc.closes() == 1
	})

	// Re-entrant Close calls must remain no-ops.
	if err := nbc.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
	if err := nbc.CloseWithError(errors.New("again")); err != nil {
		t.Fatalf("third Close returned error: %v", err)
	}
	waitFor(t, "OnClose stable after re-entrant Close", time.Second, func() bool {
		return cc.closes() == 1
	})

	stopEngineWithWatchdog(t, g)
}
