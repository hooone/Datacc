package wal

// WAL 数据的读写单元
type WALEntry interface {
	Encode(dst []byte) ([]byte, error)
	MarshalBinary() ([]byte, error)
	UnmarshalBinary(b []byte) error
	MarshalSize() int
}
