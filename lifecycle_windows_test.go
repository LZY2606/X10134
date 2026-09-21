// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build windows
// +build windows

package nbio

// The std/windows implementation writes synchronously and has no write list.
// Tests only assert the weak property there (queuing not guaranteed).
func writeListLen(c *Conn) int { return 0 }

// The std/windows backend writes synchronously through net.Conn and has no
// write-ready queue.
const writeQueueSupported = false
