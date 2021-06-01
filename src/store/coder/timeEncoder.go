package coder

import (
	"encoding/binary"
	"math"

	"github.com/jwilder/encoding/simple8b"
)

const (
	// 未压缩
	timeUncompressed = 0
	// simple8b方式压缩
	timeCompressedPackedSimple = 1
	// 定长方式压缩
	timeCompressedRLE = 2
)

type TimeEncoder interface {
	Write(t int64)
	Bytes() ([]byte, error)
	Reset()
}

type encoder struct {
	// 编码前的时间戳
	ts []uint64
	// 编码后生成的数据块
	bytes []byte
	enc   *simple8b.Encoder
}

func NewTimeEncoder(sz int) TimeEncoder {
	return &encoder{
		ts:  make([]uint64, 0, sz),
		enc: simple8b.NewEncoder(),
	}
}

// 添加要编码的时间戳
func (e *encoder) Write(t int64) {
	e.ts = append(e.ts, uint64(t))
}

// 分析数据，得到数据集的特征参数
func (e *encoder) reduce() (max, min, divisor uint64, rle bool, deltas []uint64) {
	deltas = e.ts
	max, min, divisor = 0, 0xFFFFFFFFFFFFFFFF, 1e12
	// 指示是否可用采用定长压缩方法的标志位
	rle = true

	for i := len(deltas) - 1; i > 0; i-- {
		// 计算时间戳的差值，减小目标数据的绝对值
		deltas[i] = deltas[i] - deltas[i-1]

		// 求最大值和最小值
		v := deltas[i]
		if v > max {
			max = v
		}
		if v < min {
			min = v
		}

		// 计算以10为底的最大公约数
		for divisor > 1 && v%divisor != 0 {
			divisor /= 10
		}

		// 确认是否可以采用定长压缩方式
		rle = i == len(deltas)-1 || (rle && deltas[i+1] == deltas[i])
	}
	return
}

// 获得数据编码
func (e *encoder) Bytes() ([]byte, error) {
	if len(e.ts) == 0 {
		return e.bytes[:0], nil
	}

	// 计算数据的特征值，用于判断编码方式
	max, min, div, rle, dts := e.reduce()

	// 采用定长方式进行压缩
	if rle && len(e.ts) > 1 {
		return e.encodeRLE(e.ts[0], e.ts[1], div, len(e.ts))
	}

	// 超过了1<<60，无法用simple8b算法压缩
	if max > simple8b.MaxValue {
		return e.encodeRaw()
	}

	// 采用simple8b算法压缩
	return e.encodePacked(div, min, dts)
}

// 重置
func (e *encoder) Reset() {
	e.ts = e.ts[:0]
	e.bytes = e.bytes[:0]
	e.enc.Reset()
}

// 采用simeple8b方法压缩
func (e *encoder) encodePacked(div, min uint64, dts []uint64) ([]byte, error) {
	// 把[1:]数据用simple8b压缩，得到deltas
	for _, v := range dts[1:] {
		if err := e.enc.Write((v - min) / div); err != nil {
			return nil, err
		}
	}
	deltas, err := e.enc.Bytes()
	if err != nil {
		return nil, err
	}

	// 初始化buffer
	sz := 8 + 8 + 1 + len(deltas)
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]

	// byte 0 高位代表编码方式
	b[0] = byte(timeCompressedPackedSimple) << 4
	// byte 0 的低位代表要末尾为0的个数，例如1000，则写入3
	b[0] |= byte(math.Log10(float64(div)))

	// 第一个时间戳，未经过压缩
	binary.LittleEndian.PutUint64(b[1:9], uint64(dts[0]))

	// 时间戳的最小值
	binary.LittleEndian.PutUint64(b[9:17], min)

	// 时间戳主体数据
	copy(b[17:], deltas)
	return b[:17+len(deltas)], nil
}

// encodeRLE 定长方式压缩编码
func (e *encoder) encodeRLE(first, delta, div uint64, n int) ([]byte, error) {
	sz := 31
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]
	// byte 0 的高位代表编码方式
	b[0] = byte(timeCompressedRLE) << 4
	// byte 0 的低位代表要末尾为0的个数，例如1000，则写入3
	b[0] |= byte(math.Log10(float64(div)))

	i := 1
	// 第一个时间戳，定长为8
	binary.LittleEndian.PutUint64(b[i:], uint64(first))
	i += 8
	// 时间戳步长，以变长编码方式转化为[]byte
	i += binary.PutUvarint(b[i:], uint64(delta/div))
	// 时间戳数量，以变长编码方式转化为[]byte
	i += binary.PutUvarint(b[i:], uint64(n))

	return b[:i], nil
}

// 不压缩，直接编码
func (e *encoder) encodeRaw() ([]byte, error) {
	sz := 1 + len(e.ts)*8
	if cap(e.bytes) < sz {
		e.bytes = make([]byte, sz)
	}
	b := e.bytes[:sz]
	b[0] = byte(timeUncompressed) << 4
	for i, v := range e.ts {
		binary.LittleEndian.PutUint64(b[1+i*8:1+i*8+8], uint64(v))
	}
	return b, nil
}
