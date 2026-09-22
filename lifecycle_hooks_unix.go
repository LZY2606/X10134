// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

// testHookFlushQueued is invoked from (*Conn).flush while the conn mutex is
// held, after it has been established that the write queue is non-empty and
// before any queued buffer is touched by the syscall. Tests use it to freeze
// the poller at a precise point of the flush sequence.
var testHookFlushQueued func(c *Conn)
