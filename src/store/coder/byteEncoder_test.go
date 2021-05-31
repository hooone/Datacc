package coder

import (
	"encoding/binary"
	"testing"
)

// Simpleb编码方式测试
func TestByteCoder_EncodeSimple8b(t *testing.T) {
	en := NewByteEncoder(0)
	// 模拟数据
	en.Write(byte(20))
	en.Write(byte(30))
	en.Write(byte(40))
	en.Write(byte(70))
	en.Write(byte(80))
	en.Write(byte(90))
	// 计算编码
	bts, err := en.Bytes()
	if err != nil {
		t.Fatalf("byte encode fail: %v", err)
	}
	if len(bts) == 0 {
		t.Fatalf("byte encode fail: length 0")
	}

	// 校验编码方式
	if (bts[0] >> 4) != byteCompressedSimple {
		t.Fatalf("byte encode method error: except 1,actual %d", bts[0]>>4)
	}
	// 校验差值的最小值
	if (bts[1]) != (10 + 128) {
		t.Fatalf("byte encode 1 error")
	}
	// 压缩的主体数据校验
	var buf [240]byte
	r := binary.BigEndian.Uint32(bts[3:])
	n, _ := Decompress(&buf, r)
	if n != 5 {
		t.Fatalf("byte encode count error")
	}
	if buf[0] != 0 {
		t.Fatalf("byte encode simple data error")
	}
}

// 定长编码测试
func TestByteCoder_EncodeRLE(t *testing.T) {
	en := NewByteEncoder(0)
	// 模拟数据
	en.Write(byte(10))
	en.Write(byte(20))
	en.Write(byte(30))
	en.Write(byte(40))
	en.Write(byte(50))
	en.Write(byte(60))

	// 计算编码
	bts, err := en.Bytes()
	if err != nil {
		t.Fatalf("byte encode fail: %v", err)
	}
	if len(bts) == 0 {
		t.Fatalf("byte encode fail: length 0")
	}

	// 校验编码方式
	if (bts[0] >> 4) != byteCompressedRLE {
		t.Fatalf("byte encode method error: except %d,actual %d", byteCompressedRLE, bts[0]>>4)
	}
}
