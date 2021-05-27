package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
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

	// 快照的数据量
	snapshotSize uint64
	// 快照标志位
	snapshotting bool
	// 快照对象
	snapshot *Cache
	// 快照时间
	lastSnapshot time.Time
}

func NewCache(maxSize uint64) *Cache {
	c := &Cache{
		maxSize:      maxSize,
		stats:        &CacheStatistics{},
		lastSnapshot: time.Now(),
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
		return fmt.Errorf("data array length not equal")
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

// Deduplicate 去重复
func (c *Cache) Deduplicate() {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()

	// 并发执行去重算法
	_ = store.apply(func(_ []byte, e *entry) error { e.deduplicate(); return nil })
}

// 获取当前Cache当前的总大小
func (c *Cache) Size() uint64 {
	return atomic.LoadUint64(&c.size) + atomic.LoadUint64(&c.snapshotSize)
}

// Snapshot 把当前的数据存入c.snapshot中，然后清空当前cache
func (c *Cache) Snapshot() (*Cache, error) {
	c.init()

	// 快照调用互锁
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshotting {
		return nil, fmt.Errorf("snapshot in progress")
	}
	c.snapshotting = true

	// 首次调用时快照初始化
	if c.snapshot == nil {
		store, err := newring(ringShards)
		if err != nil {
			return nil, err
		}
		c.snapshot = &Cache{
			store: store,
		}
	}

	// 返回未处理完毕的快照
	if c.snapshot.Size() > 0 {
		return c.snapshot, nil
	}

	// 将当前的store转入下线，转入快照
	c.snapshot.store, c.store = c.store, c.snapshot.store

	// 将当前Cache的大小赋值给快照
	snapshotSize := c.Size()
	atomic.StoreUint64(&c.snapshot.size, snapshotSize)
	atomic.StoreUint64(&c.snapshotSize, snapshotSize)

	// 重置当前Cache的工作区
	c.store.reset()
	atomic.StoreUint64(&c.size, 0)

	// 更新统计值
	c.lastSnapshot = time.Now()

	return c.snapshot, nil
}
