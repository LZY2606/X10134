// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build windows
// +build windows

package nbio

// Test-only synchronization points. The windows path only needs the Stop
// hook; the *nix hooks live in hooks_unix.go.
var (
	testHookStopAfterStoppingSet func()
)
