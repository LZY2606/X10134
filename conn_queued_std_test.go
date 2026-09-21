// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build windows
// +build windows

package nbio

import "testing"

// Windows std conns write synchronously without a visible write cache;
// report 1 so callers proceed to the drain.
func queuedLen(_ *testing.T, c *Conn) int {
	if c.closed {
		return 0
	}
	return 1
}
