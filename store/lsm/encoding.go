package lsm

import (
	"encoding/binary"
	"runtime"

	"github.com/hooone/datacc/common/pool"
	"github.com/hooone/datacc/store/coder"
)

var (
	// encoder pools
	timeEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return coder.NewTimeEncoder(sz)
	})
	byteEncoderPool = pool.NewGeneric(runtime.NumCPU(), func(sz int) interface{} {
		return coder.NewByteEncoder(sz)
	})
)

// 提前完成池中encoder和decoder的初始化
func init() {
	vals := make([]interface{}, 0, runtime.NumCPU())
	for _, p := range []*pool.Generic{
		timeEncoderPool,
		byteEncoderPool,
	} {
		vals = vals[:0]
		// 从池中n取出coder,n为最大允许的数量
		for i := 0; i < runtime.NumCPU(); i++ {
			v := p.Get(DefaultMaxPointsPerBlock)
			vals = append(vals, v)
		}
		// 释放coder
		for _, v := range vals {
			p.Put(v)
		}
	}
}

// 取出TimeEncoder。若所有encoder都被占用，则会等待
func getTimeEncoder(sz int) coder.TimeEncoder {
	// 从timeEncoderPool中取出的一定是TimeEncoder，所以可以直接断言
	x := timeEncoderPool.Get(sz).(coder.TimeEncoder)
	x.Reset()
	return x
}

// 释放TimeEncoder
func putTimeEncoder(enc coder.TimeEncoder) { timeEncoderPool.Put(enc) }

// 取出ByteEncoder。若所有encoder都被占用，则会等待
func getByteEncoder(sz int) coder.ByteEncoder {
	x := byteEncoderPool.Get(sz).(coder.ByteEncoder)
	x.Reset()
	return x
}

// 释放ByteEncoder
func putByteEncoder(enc coder.ByteEncoder) { byteEncoderPool.Put(enc) }

// 将明码数据打包成数据块
func encodeByteBlockUsing(buf []byte, values []coder.Value, tenc coder.TimeEncoder, venc coder.ByteEncoder) ([]byte, error) {
	tenc.Reset()
	venc.Reset()

	// 把数据写入编码器
	for _, v := range values {
		tenc.Write(v.UnixNano)
		venc.Write(v.Value)
	}

	// 获得编码后的数据切片
	tb, err := tenc.Bytes()
	if err != nil {
		return nil, err
	}
	vb, err := venc.Bytes()
	if err != nil {
		return nil, err
	}

	// 打包时间数据切片和内容数据切片
	return packBlock(buf, tb, vb), nil
}

// 打包ts和values切片，在头部写入一个变长值代表时间戳的长度
func packBlock(buf []byte, ts []byte, values []byte) []byte {
	// 数据长度校验
	sz := binary.MaxVarintLen64 + len(ts) + len(values)
	if cap(buf) < sz {
		buf = make([]byte, sz)
	}
	b := buf[:sz]

	// 数据块头部是时间戳的长度
	i := binary.PutUvarint(b[0:binary.MaxVarintLen64], uint64(len(ts)))
	// 复制时间戳数据
	copy(b[i:], ts)
	// 复制内容数据
	copy(b[i+len(ts):], values)
	return b[:i+len(ts)+len(values)]
}
