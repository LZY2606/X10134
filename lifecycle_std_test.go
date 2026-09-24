//go:build windows
// +build windows

package nbio

// testConnHasWriteQueue reports whether Conn caches unflushed data in a
// user-space write list. The std poller writes synchronously and has no
// queue, so queue-specific assertions are replaced by a synchronous
// write/close flow on this platform.
const testConnHasWriteQueue = false

// testWritevReportsFull reports whether Conn.Writev calls OnWrittenSize
// for buffers that were written in full. The std poller reports all
// written writev buffers, including fully written ones.
const testWritevReportsFull = true
