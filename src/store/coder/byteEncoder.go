package coder

import (
	"encoding/binary"
)

const (
	// Simple8b压缩方式
	byteCompressedSimple = 1
	// 定长压缩方式
	byteCompressedRLE = 2
)

type ByteEncoder struct {
	prev   byte
	rle    bool
	values []byte
}

func NewByteEncoder(sz int) ByteEncoder {
	return ByteEncoder{
		rle:    true,
		values: make([]byte, 0, sz),
	}
}

// 添加要写入的数据
func (e *ByteEncoder) Write(v byte) {
	// 滚动方式记录数据差值
	delta := v - e.prev
	e.prev = v
	//避免负数的影响
	enc := delta + 128

	// 判断能否用定长方式
	if len(e.values) > 1 {
		e.rle = e.rle && byte(e.values[len(e.values)-1]) == enc
	}

	e.values = append(e.values, enc)
}

// 获得数据编码
func (e *ByteEncoder) Bytes() ([]byte, error) {
	// Only run-length encode if it could reduce storage size.
	if e.rle && len(e.values) > 2 {
		return e.encodeRLE()
	}

	return e.encodePacked()
}

// 重置
func (e *ByteEncoder) Reset() {
	e.prev = 0
	e.rle = true
	e.values = e.values[:0]
}

// 定长方式数据编码
func (e *ByteEncoder) encodeRLE() ([]byte, error) {
	var b [31]byte
	// byte 0 记录编码类型
	b[0] = byte(byteCompressedRLE) << 4
	// byte 1 记录第一个值
	b[1] = byte(e.values[0])
	// byte 2 记录数据差值
	b[2] = byte(e.values[1])
	// byte 3: 记录数据个数
	i := 3 + binary.PutUvarint(b[3:], uint64(len(e.values)-1))
	return b[:i], nil
}

// simple8b方式数据编码
func (e *ByteEncoder) encodePacked() ([]byte, error) {
	if len(e.values) == 0 {
		return nil, nil
	}
	// 获得数据最小值
	min := byte(255)
	for _, i := range e.values[1:] {
		if i < min {
			min = i
		}
	}
	// 把数据主体从差值变为差值减去最小值
	for i := 1; i < len(e.values); i++ {
		e.values[i] -= min
	}

	// 采用simple8b算法压缩
	encoded, err := CompressAll(e.values[1:])
	if err != nil {
		return nil, err
	}

	b := make([]byte, 3+len(encoded)*4)
	// byte 0 的高位代表压缩方式
	b[0] = byte(byteCompressedSimple) << 4
	// byte 1 记录差值最小值
	b[1] = min
	// byte 2 记录未压缩的第一个值
	b[2] = e.values[0] - min

	// 填入压缩后的数据
	for i, v := range encoded {
		binary.BigEndian.PutUint32(b[3+i*4:3+i*4+4], v)
	}
	return b, nil
}
