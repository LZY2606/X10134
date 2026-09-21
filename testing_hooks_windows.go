// Copyright 2020 lesismal. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build windows
// +build windows

package nbio

// resetTestHooksPlatform is a no-op on windows.
func resetTestHooksPlatform() {}
