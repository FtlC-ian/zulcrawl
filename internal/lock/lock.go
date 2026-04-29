// Package lock provides a simple exclusive file lock that prevents two
// concurrent zulcrawl sync processes from writing the same archive at the
// same time.
//
// Locking is advisory (syscall.Flock) and non-blocking: if the lock is
// already held the caller receives an error immediately rather than waiting.
package lock

import (
	"fmt"
	"os"
	"syscall"
)

// Lock holds an open file descriptor with an exclusive advisory flock.
type Lock struct {
	f *os.File
}

// Acquire opens (or creates) the file at path and takes an exclusive
// non-blocking flock on it.  If another process or goroutine already holds
// the lock, Acquire returns an error immediately – it never blocks.
//
// The caller must call Release when the protected work is done.
func Acquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("lock: open %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another zulcrawl sync is already running (lock file: %s)", path)
	}
	return &Lock{f: f}, nil
}

// Release unlocks and removes the lock file.  It is safe to call Release
// more than once; the second call is a no-op.
func (l *Lock) Release() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	name := l.f.Name()
	_ = l.f.Close()
	_ = os.Remove(name)
	l.f = nil
}
