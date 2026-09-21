// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

package nbio

// This file holds unexported synchronization hooks that are only ever
// assigned by tests of this package. They are nil in production builds
// and add no public API surface. They exist so that lifecycle tests can
// pin down deterministic interleavings between Engine.Stop, connection
// acceptance and connection closure without relying on sleeps or
// scheduling luck.

// testHookBeforeShutdownWait, when non-nil, is called by Engine.Stop
// after the engine has been marked as shutting down, the listeners have
// been stopped and the connection snapshot has been taken, right before
// waiting for all connections to close.
var testHookBeforeShutdownWait func()
