package lock_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/FtlC-ian/zulcrawl/internal/lock"
)

func TestAcquireRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	l, err := lock.Acquire(path)
	if err != nil {
		t.Fatalf("expected acquire to succeed, got: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Errorf("lock file should exist after Acquire, got: %v", err)
	}
	l.Release()
}

func TestDoubleLockFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	l1, err := lock.Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}
	defer l1.Release()

	_, err = lock.Acquire(path)
	if err == nil {
		t.Fatal("expected second Acquire to fail while first lock is held")
	}
}

func TestLockFileRemainsOnRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	l, err := lock.Acquire(path)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	l.Release()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("lock file should remain after Release, got: %v", err)
	}
}

func TestReleaseIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	l, err := lock.Acquire(path)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	l.Release()
	l.Release() // should not panic or error
}

func TestRelockAfterRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.lock")

	l1, err := lock.Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire failed: %v", err)
	}
	l1.Release()

	l2, err := lock.Acquire(path)
	if err != nil {
		t.Fatalf("re-Acquire after Release failed: %v", err)
	}
	l2.Release()
}
