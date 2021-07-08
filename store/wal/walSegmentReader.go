package wal

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/hooone/datacc/store/coder"

	"github.com/golang/snappy"
)

type WALSegmentReader struct {
	rc    io.ReadCloser
	r     *bufio.Reader
	entry WALEntry
	n     int64
	err   error
}

func NewWALSegmentReader(r io.ReadCloser) *WALSegmentReader {
	return &WALSegmentReader{
		rc: r,
		r:  bufio.NewReader(r),
	}
}

// 清除已经解析的数据，切换文件
func (r *WALSegmentReader) Reset(rc io.ReadCloser) {
	r.rc = rc
	r.r.Reset(rc)
	r.entry = nil
	r.n = 0
	r.err = nil
}

// 解析字节流到WALEntry
func (r *WALSegmentReader) Next() bool {
	var nReadOK int

	// 读取块长度
	var lv [4]byte
	n, err := io.ReadFull(r.r, lv[:])
	if err == io.EOF {
		return false
	}
	if err != nil {
		r.err = err
		return true
	}
	nReadOK += n
	length := binary.BigEndian.Uint32(lv[:])

	// 获得byte buffer
	b := bytesPool.Get(int(length))
	defer bytesPool.Put(b)

	// 读取经过压缩的数据
	n, err = io.ReadFull(r.r, b[:length])
	if err != nil {
		r.err = err
		return true
	}
	nReadOK += n

	// 获得解压用的byte buffer
	decLen, err := snappy.DecodedLen(b[:length])
	if err != nil {
		r.err = err
		return true
	}
	decBuf := bytesPool.Get(decLen)
	defer bytesPool.Put(decBuf)

	// 解压数据
	data, err := snappy.Decode(decBuf, b[:length])
	if err != nil {
		r.err = err
		return true
	}

	// 把数据解析成WALEntry
	r.entry = &WriteWALEntry{
		Values: make(map[uint32][]coder.Value),
	}
	r.err = r.entry.UnmarshalBinary(data)
	if r.err == nil {
		r.n += int64(nReadOK)
	}

	return true
}

func (r *WALSegmentReader) Read() (WALEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.entry, nil
}
func (r *WALSegmentReader) Count() int64 {
	return r.n
}

func (r *WALSegmentReader) Error() error {
	return r.err
}

func (r *WALSegmentReader) Close() error {
	if r.rc == nil {
		return nil
	}
	err := r.rc.Close()
	r.rc = nil
	return err
}
