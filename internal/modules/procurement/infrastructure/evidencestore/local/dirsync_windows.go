//go:build windows

package local

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// syncDirectory flushes directory metadata on filesystems that support it.
// Filesystems that reject directory handles fail closed; file Sync alone does
// not guarantee durability of a newly created or renamed directory entry.
func syncDirectory(path string) error {
	handle, err := windows.CreateFile(
		windows.StringToUTF16Ptr(path),
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil,
		windows.OPEN_EXISTING,
		windows.FILE_FLAG_BACKUP_SEMANTICS,
		0,
	)
	if err != nil {
		return fmt.Errorf("open directory for metadata flush: %w", err)
	}
	defer windows.CloseHandle(handle)
	if err := windows.FlushFileBuffers(handle); err != nil {
		return fmt.Errorf("flush directory metadata: %w", err)
	}
	return nil
}
