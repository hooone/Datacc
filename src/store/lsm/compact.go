package lsm

import (
	"datacc/store/cache"
	"fmt"
	"sync"
)

const DefaultMaxPointsPerBlock = 1

type Compactor struct {
	// 工作状态锁
	mu sync.RWMutex

	// 快照状态控制
	snapshotsEnabled   bool
	snapshotsInterrupt chan struct{}
}

// 将Cache快照并发写入TSM文件.
func (c *Compactor) WriteSnapshot(che *cache.Cache) ([]string, error) {
	// 状态检查，用于优雅退出
	c.mu.RLock()
	enabled := c.snapshotsEnabled
	intC := c.snapshotsInterrupt
	c.mu.RUnlock()
	if !enabled {
		return nil, fmt.Errorf("snapshots disabled")
	}

	// 并发方式参数预留
	concurrency := 1
	throttle := true
	splits := []*cache.Cache{che}

	// 定义写入结果 内部类
	type res struct {
		files []string
		err   error
	}

	// 并发调用文件写入函数
	resC := make(chan res, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(sp *cache.Cache) {
			iter := NewCacheKeyIterator(sp, DefaultMaxPointsPerBlock, intC)
			files, err := c.writeNewFiles(c.FileStore.NextGeneration(), 0, nil, iter, throttle)
			resC <- res{files: files, err: err}

		}(splits[i])
	}

	// 处理并发写入结果
	var err error
	files := make([]string, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		result := <-resC
		if result.err != nil {
			err = result.err
		}
		files = append(files, result.files...)
	}

	// 再次检查快照功能是否被关闭
	c.mu.Lock()
	enabled = c.snapshotsEnabled
	c.mu.Unlock()
	if !enabled {
		return nil, fmt.Errorf("snapshots disabled")
	}

	return files, err
}
