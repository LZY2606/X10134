package nbio

import (
	"fmt"
	"net"
	"runtime/debug"
	"sync/atomic"
	"testing"
)

func TestDbgTraceDelete(t *testing.T) {
	var n int64
	g := NewEngine(Config{Network: "tcp", Addrs: []string{"127.0.0.1:0"}, NPoller: 2})
	allow := make(chan struct{})
	entered := make(chan struct{})
	var target connTarget
	g.OnOpen(func(c *Conn) {
		if !target.is(c) {
			return
		}
		close(entered)
		<-allow
	})
	// wrap OnClose via direct assignment to trace
	userClose := func(c *Conn, err error) {}
	_ = userClose
	g.OnClose(func(c *Conn, err error) {
		if target.is(c) {
			atomic.AddInt64(&n, 1)
			fmt.Printf("=== OnClose #%d err=%v\n%s\n", atomic.LoadInt64(&n), err, debug.Stack())
		}
	})
	g.Start()
	raw, _ := net.Dial("tcp", g.Addrs[0])
	nbc, _ := NBConn(raw)
	target.set(nbc)
	go func() { _, _ = g.AddConn(nbc) }()
	<-entered
	nbc.Close()
	close(allow)
	waitFor(t, "", time2(), func() bool { return atomic.LoadInt64(&n) >= 2 })
	g.Stop()
}

func time2() (d testDuration) { return }
