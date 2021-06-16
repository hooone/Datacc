package wal

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
)

type WALSegmentWriter interface {
	Write(compressed []byte) error
	getSize() int
	setSize(sz int)
	sync() error
	close() error
}
type walSegmentWriter struct {
	bw   *bufio.Writer
	w    io.WriteCloser
	size int
}

func NewWALSegmentWriter(w io.WriteCloser) WALSegmentWriter {
	return &walSegmentWriter{
		bw: bufio.NewWriterSize(w, 16*1024),
		w:  w,
	}
}

// 数据写入wal文件
func (w *walSegmentWriter) Write(compressed []byte) error {
	// 写入压缩数据块的长度
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(len(compressed)))
	if _, err := w.bw.Write(buf[:]); err != nil {
		return err
	}

	// 写入压缩数据块
	if _, err := w.bw.Write(compressed); err != nil {
		return err
	}

	// 记录写入数量
	w.size += len(buf) + len(compressed)
	return nil
}

func (w *walSegmentWriter) getSize() int {
	return w.size
}

func (w *walSegmentWriter) setSize(sz int) {
	w.size = sz
}

// 数据刷入操作系统
func (w *walSegmentWriter) Flush() error {
	return w.bw.Flush()
}

// 数据刷入硬盘
func (w *walSegmentWriter) sync() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}

	if f, ok := w.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (w *walSegmentWriter) close() error {
	if err := w.Flush(); err != nil {
		return err
	}
	return w.w.Close()
}
