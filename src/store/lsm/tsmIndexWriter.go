package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

const (
	// 一个文件中每个index最多支持有65536个block，所以数量位占2个byte
	indexCountSize = 2
	// 每个block会有一个IndexEntry，指示IndexEntry的大小
	indexEntrySize = 28
)

type IndexWriter interface {
	Add(key uint32, minTime, maxTime int64, offset int64, size uint32)
	KeyCount() int
	Size() uint32
	WriteTo(w io.Writer) (int64, error)
	Remove() error
	Close() error
}
type directIndex struct {
	// 编码用的buf
	buf *bytes.Buffer
	// 写入buf的writer
	w *bufio.Writer
	// 文件writer的sync接口
	f syncer

	// 当前key
	key uint32
	// 当前key的index数据缓存
	indexEntries *indexEntries

	// 写入的key的数量
	keyCount int
	// 写入的key的大小
	size uint32
}

func NewIndexWriter() IndexWriter {
	buf := bytes.NewBuffer(make([]byte, 0, 1024*1024))
	return &directIndex{buf: buf, w: bufio.NewWriter(buf)}
}

func (d *directIndex) Add(key uint32, minTime, maxTime int64, offset int64, size uint32) {
	// 数据第一次写入
	if d.key == 0 {
		// 初始化index缓存
		d.key = key
		if d.indexEntries == nil {
			d.indexEntries = &indexEntries{}
		}

		// 填入当前block的属性
		d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{
			MinTime: minTime,
			MaxTime: maxTime,
			Offset:  offset,
			Size:    size,
		})

		// 数量统计
		d.size += uint32(4)
		d.size += indexCountSize
		d.size += indexEntrySize
		d.keyCount++
		return
	}

	// 相同key的第二次写入
	if d.key == key {
		// 填入当前block的属性
		d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{
			MinTime: minTime,
			MaxTime: maxTime,
			Offset:  offset,
			Size:    size,
		})

		// 数量统计
		d.size += indexEntrySize
	} else {
		// key切换
		// 把当前key进行编码，并写入buffer
		d.encode(d.w)
		d.key = key

		// 填入当前block的属性
		d.indexEntries.entries = append(d.indexEntries.entries, IndexEntry{
			MinTime: minTime,
			MaxTime: maxTime,
			Offset:  offset,
			Size:    size,
		})

		// 数量统计
		d.size += indexCountSize
		d.size += indexEntrySize
		d.keyCount++
	}
}
func (d *directIndex) WriteTo(w io.Writer) (int64, error) {
	// 把当前key所对应的index缓存写入buffer
	if _, err := d.encode(d.w); err != nil {
		return 0, err
	}
	if err := d.w.Flush(); err != nil {
		return 0, err
	}

	// 把buffer写到文件
	return copyBuffer(d.f, w, d.buf, nil)
}

func (d *directIndex) KeyCount() int {
	return d.keyCount
}
func (d *directIndex) Size() uint32 {
	return d.size
}

// 对当前key进行编码，并写入buffer
func (d *directIndex) encode(w io.Writer) (int64, error) {
	var (
		n   int
		err error
		kbf [4]byte
		buf [2]byte
		N   int64
	)

	// 数据状态判断
	if d.key == 0 {
		return 0, nil
	}
	key := d.key
	entries := d.indexEntries
	if !sort.IsSorted(entries) {
		sort.Sort(entries)
	}

	// 写入当前key
	binary.BigEndian.PutUint32(kbf[0:4], key)
	if n, err = w.Write(kbf[0:4]); err != nil {
		return int64(n) + N, fmt.Errorf("write: writer key error: %v", err)
	}
	N += int64(n)

	// 写入block数量
	binary.BigEndian.PutUint16(buf[0:2], uint16(entries.Len()))
	if n, err = w.Write(buf[0:2]); err != nil {
		return int64(n) + N, fmt.Errorf("write: writer block type and count error: %v", err)
	}
	N += int64(n)

	// 写入每个block的信息
	var n64 int64
	if n64, err = entries.WriteTo(w); err != nil {
		return n64 + N, fmt.Errorf("write: writer entries error: %v", err)
	}
	N += n64

	// 数据重置
	d.key = 0
	d.indexEntries.entries = d.indexEntries.entries[:0]

	return N, nil

}

// 把src分批复制到dst
func copyBuffer(f syncer, dst io.Writer, src io.Reader, buf []byte) (written int64, err error) {
	if buf == nil {
		buf = make([]byte, 32*1024)
	}
	var lastSync int64
	for {
		// 读数据
		nr, er := src.Read(buf)
		if nr > 0 {
			// 写数据
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}

			// 定时刷盘
			if written-lastSync > fsyncEvery {
				if err := f.Sync(); err != nil {
					return 0, err
				}
				lastSync = written
			}

			// 异常判断
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		// 数据读取完成
		if er != nil {
			if er != io.EOF {
				err = er
			}
			break
		}
	}
	return written, err
}

func (d *directIndex) Remove() error {
	return nil
}

func (d *directIndex) Close() error {
	if err := d.w.Flush(); err != nil {
		return err
	}
	return nil
}
