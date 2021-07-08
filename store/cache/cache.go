package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hooone/datacc/store/coder"
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
	// 最近写入时间
	lastWriteTime time.Time

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

	c.mu.Lock()
	c.lastWriteTime = time.Now()
	c.mu.Unlock()

	return nil
}

// 批量写入
func (c *Cache) WriteMulti(values map[uint32][]coder.Value) error {
	c.init()

	// 写入数据的大小校验
	var addedSize uint64
	for _, v := range values {
		addedSize += uint64(coder.Values(v).Size())
	}
	limit := c.maxSize
	n := c.Size() + addedSize
	if limit > 0 && n > limit {
		atomic.AddInt64(&c.stats.WriteErr, 1)
		return ErrCacheMemorySizeLimitExceeded(n, limit)
	}

	var werr error
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()

	// 数据写入和数量统计
	atomic.AddUint64(&c.size, addedSize)
	for k, v := range values {
		newKey, err := store.writeValues(k, v)
		if err != nil {
			werr = err
			addedSize -= uint64(coder.Values(v).Size())
			atomic.AddUint64(&c.size, ^(uint64(coder.Values(v).Size()) - 1))
		}
		if newKey {
			addedSize += uint64(4)
			atomic.AddUint64(&c.size, 4)
		}
	}

	// 更新stat
	if werr != nil {
		atomic.AddInt64(&c.stats.WriteErr, 1)
	}
	atomic.AddInt64(&c.stats.MemSizeBytes, int64(addedSize))
	atomic.AddInt64(&c.stats.WriteOK, 1)

	c.mu.Lock()
	c.lastWriteTime = time.Now()
	c.mu.Unlock()

	return werr
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

// 获得当前cache中的所有key
func (c *Cache) Keys() []uint32 {
	c.mu.RLock()
	store := c.store
	c.mu.RUnlock()
	return store.keys(true)
}

// 返回当前cache和快照中的所有数据
func (c *Cache) Values(key uint32) coder.Values {
	var snapshotEntries *entry

	// 获得cache和快照中的相关entry
	c.mu.RLock()
	e := c.store.entry(key)
	if c.snapshot != nil {
		snapshotEntries = c.snapshot.store.entry(key)
	}
	c.mu.RUnlock()
	if e == nil {
		if snapshotEntries == nil {
			return nil
		}
	} else {
		e.deduplicate()
	}

	// entry打包并统计数量
	var entries []*entry
	sz := 0
	if snapshotEntries != nil {
		snapshotEntries.deduplicate() // guarantee we are deduplicated
		entries = append(entries, snapshotEntries)
		sz += snapshotEntries.count()
	}
	if e != nil {
		entries = append(entries, e)
		sz += e.count()
	}

	// 如果Cache和快照中都没有找到数据
	if sz == 0 {
		return nil
	}

	// 返回副本
	values := make(coder.Values, sz)
	n := 0
	for _, e := range entries {
		e.mu.RLock()
		n += copy(values[n:], e.values)
		e.mu.RUnlock()
	}
	values = values[:n]
	values = values.Deduplicate()

	return values
}
