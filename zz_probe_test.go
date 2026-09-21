package nbio

import (
	"testing"
	"time"
)

func TestProbeStopPollers(t *testing.T) {
	g := NewEngine(Config{Network: "tcp", Addrs: []string{"127.0.0.1:0"}, NPoller: 3})
	if err := g.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { g.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop blocked >5s")
	}
	time.Sleep(300 * time.Millisecond)
	if s := nbioGoroutineStacks(); s != "" {
		t.Fatalf("pollers alive: %s", s)
	}
}
