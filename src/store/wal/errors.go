package wal

import "fmt"

var (
	// ErrWALClosed is returned when attempting to write to a closed WAL file.
	ErrWALClosed = fmt.Errorf("WAL closed")

	// ErrWALCorrupt is returned when reading a corrupt WAL entry.
	ErrWALCorrupt = fmt.Errorf("corrupted WAL entry")
)
