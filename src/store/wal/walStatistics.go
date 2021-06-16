package wal

type WALStatistics struct {
	OldBytes     int64
	CurrentBytes int64
	WriteOK      int64
	WriteErr     int64
}
