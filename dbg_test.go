package nbio

import (
	"net"
	"sync"
	"testing"
	"time"
)

func TestDbgBlockedOpenClose(t *testing.T) {
	var cc = newConnCounters()
	allow := make(chan struct{})
	entered := make(chan struct{})
	var target connTarget

	g := newLifecycleEngine(t, 2,
		func(c *Conn) {
			if !target.is(c) {
				return
			}
			cc.onOpen(c)
			close(entered)
			<-allow
		},
		cc.onData, cc.onClose, nil)

	raw, _ := net.Dial("tcp", g.Addrs[0])
	nbc, _ := NBConn(raw)
	target.set(nbc)
	addDone := make(chan struct{})
	go func() { _, _ = g.AddConn(nbc); close(addDone) }()
	<-entered
	t.Logf("blocked, calling close")
	if err := nbc.Close(); err != nil {
		t.Logf("close err %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	t.Logf("while blocked opens=%d closes=%d closed=%v", cc.opens(), cc.closes(), nbc.closed)
	close(allow)
	<-addDone
	time.Sleep(200 * time.Millisecond)
	t.Logf("after open done opens=%d closes=%d closed=%v connsUnix[fd]=%v",
		cc.opens(), cc.closes(), nbc.closed, g.connsUnix[nbc.fd] != nil)
	var wg sync.WaitGroup
	_ = wg
	stopEngineWithWatchdog(t, g)
}
