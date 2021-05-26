package cache

import "github.com/cespare/xxhash"

type ring struct {
	// 存储分区容器
	partitions []*partition
}

func newring(n int) (*ring, error) {
	r := ring{
		partitions: make([]*partition, n),
	}

	// 初始化每个分区
	for i := 0; i < len(r.partitions); i++ {
		r.partitions[i] = &partition{
			store: make(map[uint32]*entry),
		}
	}
	return &r, nil
}

// 根据key的哈希值确定该key被保存在哪个分区
func (r *ring) getPartition(key uint32) *partition {
	return r.partitions[int(xxhash.Sum64(int32tobytes(key))%uint64(len(r.partitions)))]
}

func (r *ring) write(key uint32, ts []int64, values []byte) (bool, error) {
	// 把数据封装成values
	vls := make([]value, len(ts))
	for i := 0; i < len(ts); i++ {
		vls[i] = value{
			UnixNano: ts[i],
			Value:    values[i],
		}
	}

	return r.getPartition(key).write(key, vls)
}

func int32tobytes(v2 uint32) []byte {
	b2 := make([]byte, 4)
	v2 = 257
	b2[3] = uint8(v2)
	b2[2] = uint8(v2 >> 8)
	b2[1] = uint8(v2 >> 16)
	b2[0] = uint8(v2 >> 24)
	return b2
}
