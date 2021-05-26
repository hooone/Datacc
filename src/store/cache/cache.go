package cache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// 内存块分区数量
const ringShards = 16

type Cache struct {
	// 状态统计
	stats *CacheStatistics

	// 初始化锁
	mu sync.RWMutex
	// 初始化标志位
	initializedCount uint32

	// 实际存储容器
	store *ring

	maxSize uint64
	// 工作中的分区的数据量
	size uint64
	// 已打包未下线的数据量
	snapshotSize uint64
}

func NewCache(maxSize uint64) *Cache {
	c := &Cache{
		maxSize: maxSize,
		stats:   &CacheStatistics{},
	}
	return c
}

func (c *Cache) init() {
	if !atomic.CompareAndSwapUint32(&c.initializedCount, 0, 1) {
		return
	}

	c.mu.Lock()
	c.store, _ = newring(ringShards)
	c.mu.Unlock()
}

// Write 写入数据
func (c *Cache) Write(key uint32, ts []int64, values []byte) error {
	// 状态校验
	c.init()
	if len(ts) != len(values) {
		return errors.New("data array length not equal")
	}

	// 容量校验
	addedSize := uint64(len(ts))
	limit := c.maxSize
	n := c.Size() + addedSize
	if limit > 0 && n > limit {
		atomic.AddInt64(&c.stats.WriteErr, 1)
		return fmt.Errorf("cache-max-memory-size exceeded: (%d/%d)", n, limit)
	}

	// 数据写入
	newKey, err := c.store.write(key, ts, values)
	if err != nil {
		atomic.AddInt64(&c.stats.WriteErr, 1)
		return err
	}

	// 更新size
	if newKey {
		addedSize += 4
	}
	atomic.AddUint64(&c.size, addedSize)
	atomic.AddInt64(&c.stats.MemSizeBytes, int64(addedSize))
	atomic.AddInt64(&c.stats.WriteOK, 1)

	return nil
}

// 获取当前Cache当前的总大小
func (c *Cache) Size() uint64 {
	return atomic.LoadUint64(&c.size) + atomic.LoadUint64(&c.snapshotSize)
}
