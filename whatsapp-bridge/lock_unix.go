//go:build !windows

package main

import (
	"os"
	"syscall"
)

// lockExclusive: one bridge per store (flock, non-blocking).
func lockExclusive(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}

func unlockFile(f *os.File) { syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
