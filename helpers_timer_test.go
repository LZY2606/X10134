package nbio

import "time"

func timerWatch(d time.Duration) <-chan struct{} {
	ch := make(chan struct{})
	time.AfterFunc(d, func() { close(ch) })
	return ch
}

func deadlineSoon() time.Time {
	return time.Now().Add(lifecycleTestTimeout)
}
