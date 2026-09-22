package nbio

import "sync"

// Test-only synchronization hooks.
//
// These hooks are unexported and are never assigned by non-test code, so they
// add a single predictable branch (a nil func call is skipped) without any
// observable behavior change to public API consumers. Lifecycle tests assign the
// variables temporarily and always restore them in cleanup.

var (
	// hookOnOpenEnter fires after the Engine has registered the new connection
	// on its wgConn counter but before the user's OnOpen handler runs.
	hookOnOpenEnter func(c *Conn)

	// hookWriteQueued fires when a Conn.Write call has just accumulated at least
	// two cached write buffers, i.e. the send queue is definitely non-empty and
	// waiting for the poller to flush. It is invoked synchronously from the
	// Write goroutine; tests use it to block the writer at a deterministic point.
	hookWriteQueued func(c *Conn)

	// hookStopAfterListeners fires inside Engine.Stop right after every listener
	// has been closed, so any subsequent accept observes a closed listener.
	hookStopAfterListeners func()

	// hookStopAfterConnsSnapshot fires inside Engine.Stop after the in-flight
	// connection tables have been snapshotted under the Engine lock.
	hookStopAfterConnsSnapshot func()

	// hookStopAfterWgConnWait fires inside Engine.Stop right after the engine
	// has waited for all in-flight OnOpen/OnClose callbacks to settle.
	hookStopAfterWgConnWait func()

	// hookStopConnIter fires inside Engine.Stop's connection-close sweep for
	// each connection present in the snapshot.
	hookStopConnIter func(c *Conn)

	// hookConnPending fires when a Close finds an in-progress addConn and
	// parks instead of tearing down. Test-only.
	hookConnPending func(c *Conn)

	// hookAddrTargets marks remote addresses whose callbacks a test
	// currently synchronizes on; it is populated before the conn exists.
	hookAddrTargets sync.Map // map[string]struct{}

	// hookMagicTargets carries 8-byte connection markers that, once seen in
	// an OnData callback, arm that connection's lifecycle hooks. Used when a
	// test needs to identify a single conn despite shared ephemeral ports.
	hookMagicTargets sync.Map // map[string]struct{}
)

var hookTargets sync.Map // map[*Conn]struct{}

func watchRemoteAddr(addr string) {
	hookAddrTargets.Store(addr, struct{}{})
}

func unwatchRemoteAddr(addr string) {
	hookAddrTargets.Delete(addr)
}

func connWatched(c *Conn) bool {
	addr := c.RemoteAddr()
	_, ok := hookAddrTargets.Load(addr.String())
	hookTargets.Store(c, struct{}{})
	return ok
}

func watchConn(c *Conn) { watchRemoteAddr(c.RemoteAddr().String()) }

func unwatchConn(c *Conn) {
	hookTargets.Delete(c)
	unwatchRemoteAddr(c.RemoteAddr().String())
}

func watchMagic(marker string) { hookMagicTargets.Store(marker, struct{}{}) }

func unwatchMagic(marker string) { hookMagicTargets.Delete(marker) }

const lifecycleMagicLen = 8

var lifecycleMagic = []byte("NB-LCYCL")

// hookOnDataMagic is installed by newLifecycleEngine.
func hookOnDataMagic(c *Conn, data []byte) {
	if len(data) >= lifecycleMagicLen {
		marker := string(data[:lifecycleMagicLen])
		if _, ok := hookMagicTargets.Load(marker); ok {
			hookAddrTargets.Store(c.RemoteAddr().String(), struct{}{})
			hookMagicTargets.Delete(marker)
		}
	}
}
