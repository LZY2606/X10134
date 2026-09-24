//go:build linux || darwin || netbsd || freebsd || openbsd || dragonfly
// +build linux darwin netbsd freebsd openbsd dragonfly

package nbio

// testConnHasWriteQueue reports whether Conn caches unflushed data in a
// user-space write list. The unix pollers queue writes and flush them on
// writable events; the std poller writes synchronously and has no queue.
const testConnHasWriteQueue = true

// testWritevReportsFull reports whether Conn.Writev calls OnWrittenSize
// for buffers that were written in full. The unix pollers only report
// partially written writev buffers; the std poller reports all of them.
const testWritevReportsFull = false
