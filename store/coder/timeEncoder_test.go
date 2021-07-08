package coder

import (
	"testing"

	"github.com/jwilder/encoding/simple8b"
)

// Simpleb编码方式测试
func TestTimeCoder_EncodeSimple8b(t *testing.T) {
	en := NewTimeEncoder(0)
	// 模拟数据
	en.Write(1000)
	en.Write(2000)
	en.Write(4000)
	en.Write(6000)
	en.Write(7000)
	en.Write(8000)

	// 计算编码
	bts, err := en.Bytes()
	if err != nil {
		t.Fatalf("timestamps encode fail: %v", err)
	}
	if len(bts) == 0 {
		t.Fatalf("timestamps encode fail: length 0")
	}

	// 校验编码方式
	if (bts[0] >> 4) != timeCompressedPackedSimple {
		t.Fatalf("timestamps encode method error: except 1,actual %d", bts[0]>>4)
	}
	// 校验末尾0的个数
	if (bts[0] & 0x0F) != 3 {
		t.Fatalf("timestamps encode div error: except 3,actual %d", bts[0]>>4)
	}
	// 校验主体数据
	dec := *simple8b.NewDecoder(bts[17:])
	dec.Next()
	v1 := dec.Read()
	if (v1) != 0 {
		t.Fatalf("timestamps encode delta error: except 0,actual %d", v1)
	}
	dec.Next()
	v2 := dec.Read()
	if (v2) != 1 {
		t.Fatalf("timestamps encode delta error: except 1,actual %d", v2)
	}
}

// 定长编码测试
func TestTimeCoder_EncodeRLE(t *testing.T) {
	en := NewTimeEncoder(0)
	// 模拟数据
	en.Write(1000)
	en.Write(2000)
	en.Write(3000)
	en.Write(4000)
	en.Write(5000)
	en.Write(6000)

	// 计算编码
	bts, err := en.Bytes()
	if err != nil {
		t.Fatalf("timestamps encode fail: %v", err)
	}
	if len(bts) == 0 {
		t.Fatalf("timestamps encode fail: length 0")
	}

	// 校验编码方式
	if (bts[0] >> 4) != timeCompressedRLE {
		t.Fatalf("timestamps encode method error: except %d,actual %d", timeCompressedRLE, bts[0]>>4)
	}
	// 校验末尾0的个数
	if (bts[0] & 0x0F) != 3 {
		t.Fatalf("timestamps encode div error: except 3,actual %d", bts[0]>>4)
	}
}
