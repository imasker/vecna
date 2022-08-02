package iface

type Lock interface {
	// LockWithRetries Acquire the lock with retry
	// key: the name of the lock,
	// value: at the nanosecond timestamp that lock needs to be released automatically
	LockWithRetries(key string, value int64) error

	// Lock Acquire the lock with once
	// key: the name of the lock,
	// value: at the nanosecond timestamp that lock needs to be released automatically
	Lock(key string, value int64) error
}
