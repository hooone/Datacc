package lsm

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"os"
)

/*
┌───────────────────┐
│      Header       │
├─────────┬─────────┤
│  Magic  │ Version │
│ 4 bytes │ 1 byte  │
└─────────┴─────────┘
┌───────────────────────────────────────────────────────────┐
│                          Blocks                           │
├───────────────────┬───────────────────┬───────────────────┤
│      Block 1      │      Block 2      │      Block N      │
├─────────┬─────────┼─────────┬─────────┼─────────┬─────────┤
│  CRC    │  Data   │  CRC    │  Data   │  CRC    │  Data   │
│ 4 bytes │ N bytes │ 4 bytes │ N bytes │ 4 bytes │ N bytes │
└─────────┴─────────┴─────────┴─────────┴─────────┴─────────┘
┌───────────────────────────────────────────────────────────┐
│                         Index                             │
├─────────┬───────┬─────────┬─────────┬────────┬────────┬───┤
│   Key   │ Count │Min Time │Max Time │ Offset │  Size  │...│
│ 4 bytes │2 bytes│ 8 bytes │ 8 bytes │8 bytes │4 bytes │   │
└─────────┴───────┴─────────┴─────────┴────────┴────────┴───┘
┌─────────┐
│ Footer  │
├─────────┤
│Index Ofs│
│ 8 bytes │
└─────────┘
*/

const (
	// 文件类型标志位
	MagicNumber uint32 = 0x16D116D0

	// 版本号
	Version byte = 1

	// Key的长度
	keyLength = 4

	// 调用强制刷盘的数据量
	fsyncEvery = 25 * 1024 * 1024
)

// TSM文件的格式化写入
type TSMWriter interface {
	WriteBlock(key uint32, minTime, maxTime int64, block []byte) error
	WriteIndex() error
	Size() uint32
	Remove() error
	Close() error
}
type tsmWriter struct {
	// 文件writer
	wrapped io.Writer
	// 文件writer的bufio
	bufw *bufio.Writer

	// 文件索引区的缓存器和写入器
	index IndexWriter
	// 写入的数据量
	n int64

	// 最近一次刷盘时的文件大小
	lastSync int64
}

type syncer interface {
	Name() string
	Sync() error
}

func NewTSMWriter(w io.Writer) (TSMWriter, error) {
	index := NewIndexWriter()
	return &tsmWriter{wrapped: w, bufw: bufio.NewWriterSize(w, 1024*1024), index: index}, nil
}

// 把一个block的数据写入文件，并缓存key等属性
func (t *tsmWriter) WriteBlock(key uint32, minTime, maxTime int64, block []byte) error {
	// 数据状态判断
	if len(block) == 0 {
		return nil
	}

	// 在对一个新的文件第一次写入时，先写文件头
	if t.n == 0 {
		if err := t.writeHeader(); err != nil {
			return err
		}
	}

	// 写入每个块头部的CRC校验码
	var checksum [crc32.Size]byte
	binary.BigEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))
	_, err := t.bufw.Write(checksum[:])
	if err != nil {
		return err
	}

	// 写入文件块数据
	n, err := t.bufw.Write(block)
	if err != nil {
		return err
	}
	n += len(checksum)

	// 把数据块的属性缓存到index缓存器中
	t.index.Add(key, minTime, maxTime, t.n, uint32(n))

	// 累计写入数量
	t.n += int64(n)

	// 如果最近一次刷盘后写入的数量超过了临界值，则调用强制刷盘
	if t.n-t.lastSync > fsyncEvery {
		if err := t.sync(); err != nil {
			return err
		}
		t.lastSync = t.n
	}

	return nil
}

// 把缓存的block属性写入文件
func (t *tsmWriter) WriteIndex() error {
	// 数据状态检测
	if t.index.KeyCount() == 0 {
		return ErrNoValues
	}

	// 先把刷盘方法通过委托方式传入index缓存器
	if f, ok := t.wrapped.(syncer); ok {
		t.index.(*directIndex).f = f
	}

	// 写入Index区数据
	if _, err := t.index.WriteTo(t.bufw); err != nil {
		return err
	}

	// 写Index的位置(Foot部分)
	var buf [8]byte
	indexPos := t.n
	binary.BigEndian.PutUint64(buf[:], uint64(indexPos))
	_, err := t.bufw.Write(buf[:])
	return err
}

// 写入文件头: 识别码和版本号
func (t *tsmWriter) writeHeader() error {
	var buf [5]byte
	binary.BigEndian.PutUint32(buf[0:4], MagicNumber)
	buf[4] = Version
	n, err := t.bufw.Write(buf[:])
	if err != nil {
		return err
	}
	t.n = int64(n)
	return nil
}

// 获得当前写入的文件大小
func (t *tsmWriter) Size() uint32 {
	return uint32(t.n) + t.index.Size()
}

// Remove 移除当前writer使用的文件
func (t *tsmWriter) Remove() error {
	if err := t.index.Remove(); err != nil {
		return err
	}

	type nameCloser interface {
		io.Closer
		Name() string
	}
	if f, ok := t.wrapped.(nameCloser); ok {
		_ = f.Close()
		return os.Remove(f.Name())
	}
	return nil
}

// 关闭文件
func (t *tsmWriter) Close() error {
	// 刷盘
	if err := t.Flush(); err != nil {
		return err
	}

	// 关闭index缓存器
	if err := t.index.Close(); err != nil {
		return err
	}

	// 释放文件
	if c, ok := t.wrapped.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// 刷盘
func (t *tsmWriter) Flush() error {
	if err := t.bufw.Flush(); err != nil {
		return err
	}

	return t.sync()
}

func (t *tsmWriter) sync() error {
	type sync interface {
		Sync() error
	}

	if f, ok := t.wrapped.(sync); ok {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}
