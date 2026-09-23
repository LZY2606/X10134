//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly

package nbio

var testAcceptAfterAddConnHook func()
var testAcceptBeforeAddConnHook func()
var testStopAfterListenerStopHook func()
var testStopAfterSnapshotHook func()
