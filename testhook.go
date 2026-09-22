package nbio

// Synchronization hooks used only by tests to build deterministic
// interleavings of Engine/Conn lifecycle events. They are never set by
// non-test code, are not part of the public API, and must stay
// unexported. Keeping them nil costs nothing in production builds.
var (
	// testHookBeforeStopWait is called by Engine.Stop right before it
	// starts waiting for all the connections to be closed.
	testHookBeforeStopWait func()
)
