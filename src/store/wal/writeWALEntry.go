package wal

import (
	"datacc/store/coder"
	"encoding/binary"
)

// ┌─────────────────────────────────────────────────────────────┐
// │                       WriteWALEntry                         │
// ├────────┬─────────┬─────────┬────────┬─────────┬─────────┬───┤
// │   Key  │  Time   │  Value  │   Key  │  Time   │  Value  │...│
// │ 4 bytes│ 8 bytes │ 1 bytes │ 4 bytes│ 8 bytes │ 1 bytes │   │
// └────────┴─────────┴─────────┴────────┴─────────┴─────────┴───┘
type WriteWALEntry struct {
	Values map[uint32][]coder.Value
	sz     int
}

// 封装成二进制
func (w *WriteWALEntry) MarshalBinary() ([]byte, error) {
	b := make([]byte, w.MarshalSize())
	return w.Encode(b)
}

// 将WriteWALEntry编码
func (w *WriteWALEntry) Encode(dst []byte) ([]byte, error) {
	// 计算编码后的长度
	encLen := w.MarshalSize()

	// 切片预处理
	if len(dst) < encLen {
		dst = make([]byte, encLen)
	} else {
		dst = dst[:encLen]
	}

	// 编码
	var n int
	for k, v := range w.Values {
		for _, vv := range v {
			// key
			binary.BigEndian.PutUint32(dst[n:n+4], k)
			n += 4
			// time
			binary.BigEndian.PutUint64(dst[n:n+8], uint64(vv.UnixNano))
			n += 8
			// value
			dst[n] = vv.Value
			n++
		}
	}

	return dst[:n], nil
}

// 解码
func (w *WriteWALEntry) UnmarshalBinary(b []byte) error {
	var i int
	lastKey := uint32(0)
	values := make([]coder.Value, 0)
	for i < len(b) {
		// 长度确认
		if i+9 > len(b) {
			return ErrWALCorrupt
		}
		// key
		key := binary.BigEndian.Uint32(b[i : i+4])
		i += 4

		// key切换时保存数据
		if lastKey != key {
			if len(values) > 0 {
				w.Values[lastKey] = values
			}
		}
		lastKey = key

		// timestamp + value
		un := int64(binary.BigEndian.Uint64(b[i : i+8]))
		i += 8
		values = append(values, coder.NewValue(un, b[i]))
		i += 1
	}
	// 解析结束时保存数据
	if len(values) > 0 {
		w.Values[lastKey] = values
	}
	return nil
}

func (w *WriteWALEntry) MarshalSize() int {
	if w.sz > 0 || len(w.Values) == 0 {
		return w.sz
	}

	encLen := 0

	for _, v := range w.Values {
		if len(v) == 0 {
			return 0
		}
		encLen += 4 * len(v) // key
		encLen += 8 * len(v) // timestamps
		encLen += 1 * len(v) // value
	}

	w.sz = encLen

	return w.sz
}
