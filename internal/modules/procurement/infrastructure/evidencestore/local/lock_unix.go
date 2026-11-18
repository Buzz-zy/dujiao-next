//go:build !windows

package local

import (
	"golang.org/x/sys/unix"
	"os"
)

func acquireFileLock(file *os.File) error {
	return unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
}
func releaseFileLock(file *os.File) error { return unix.Flock(int(file.Fd()), unix.LOCK_UN) }
