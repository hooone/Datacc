package cache

import (
	"datacc/store/coder"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash"
)

type ring struct {
	// 存储的key的数量
	keysHint int64

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

// 数据写入
func (r *ring) write(key uint32, ts []int64, values []byte) (bool, error) {
	// 把数据封装成values
	vls := make([]coder.Value, len(ts))
	for i := 0; i < len(ts); i++ {
		vls[i] = coder.Value{
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

// 数据清空
func (r *ring) reset() {
	for _, partition := range r.partitions {
		partition.reset()
	}
	r.keysHint = 0
}

// 对所有的分区异步执行传入的方法
func (r *ring) apply(f func([]byte, *entry) error) error {

	var (
		// 计数锁
		wg sync.WaitGroup
		// 异步获取可能的error
		res = make(chan error, len(r.partitions))
	)

	// 异步执行
	for _, p := range r.partitions {
		wg.Add(1)

		go func(p *partition) {
			defer wg.Done()

			p.mu.RLock()
			for k, e := range p.store {
				if err := f(int32tobytes(k), e); err != nil {
					res <- err
					p.mu.RUnlock()
					return
				}
			}
			p.mu.RUnlock()
		}(p)
	}

	// 等待协程完成
	go func() {
		wg.Wait()
		close(res)
	}()

	// 记录error
	for err := range res {
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ring) entry(key uint32) *entry {
	return r.getPartition(key).entry(key)
}

// 返回所有key并排序
func (r *ring) keys(sorted bool) []uint32 {
	keys := make([]uint32, 0, atomic.LoadInt64(&r.keysHint))
	for _, p := range r.partitions {
		keys = append(keys, p.keys()...)
	}

	// 排序
	if sorted {
		sort.Sort(uint32Slices(keys))
	}
	return keys
}

type uint32Slices []uint32

func (a uint32Slices) Len() int           { return len(a) }
func (a uint32Slices) Less(i, j int) bool { return a[i] < a[j] }
func (a uint32Slices) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
