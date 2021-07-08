package lsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/hooone/datacc/common/limiter"
	"github.com/hooone/datacc/store/cache"
)

const DefaultMaxPointsPerBlock = 240 * 8
const maxTSMFileSize = uint32(2048 * 1024 * 1024) // 2GB

const (
	CompactionTempExtension = "tmp"
	TSMFileExtension        = "tsm"
)

type Compactor struct {
	// 目标文件目录
	Dir string
	// 工作状态锁
	mu sync.RWMutex
	// 写入限流器
	RateLimit limiter.Rate

	// 获得文件版本号，用于生成文件名
	FileStore interface {
		NextGeneration() int
	}

	// 文件层级压缩状态控制
	compactionsEnabled bool
	// 快照状态控制
	snapshotsEnabled   bool
	snapshotsInterrupt chan struct{}
}

func NewCompactor() *Compactor {
	return &Compactor{}
}
func (c *Compactor) Open() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.snapshotsEnabled || c.compactionsEnabled {
		return
	}

	c.snapshotsEnabled = true
	c.compactionsEnabled = true
	c.snapshotsInterrupt = make(chan struct{})
}

// 将Cache快照写入TSM文件.
func (c *Compactor) WriteSnapshot(che *cache.Cache) ([]string, error) {
	// 状态检查，用于优雅退出
	c.mu.RLock()
	enabled := c.snapshotsEnabled
	intC := c.snapshotsInterrupt
	c.mu.RUnlock()
	if !enabled {
		return nil, errSnapshotsDisabled
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
		return nil, errSnapshotsDisabled
	}

	return files, err
}

// 把KeyIterator中的所有数据写入文件
func (c *Compactor) writeNewFiles(generation, sequence int, src []string, iter KeyIterator, throttle bool) ([]string, error) {
	var files []string
	for {
		sequence++

		// 生成文件名。先把数据写成.tmp，写入完成后重命名
		fileName := filepath.Join(c.Dir, formatFileName(generation, sequence))

		// 尽可能多的写入
		err := c.write(fileName, iter, throttle)

		// 当把文件写满时，切换到下一个文件，sequence++
		if err == errMaxFileExceeded || err == ErrMaxBlocksExceeded {
			files = append(files, fileName)
			continue
		} else if err == ErrNoValues {
			// 数据已经写完，这次循环没有写入数据
			if err := os.RemoveAll(fileName); err != nil {
				return nil, err
			}
			break
		} else if _, ok := err.(errCompactionInProgress); ok {
			// 文件占用异常
			return nil, err
		} else if err != nil {
			// 未知异常，移除本次写的所有.tmp文件
			for _, f := range files {
				if err := os.RemoveAll(f); err != nil {
					return nil, err
				}
			}
			if err := os.RemoveAll(fileName); err != nil {
				return nil, err
			}
			return nil, err
		}

		files = append(files, fileName)
		break
	}
	return files, nil
}

// 把数据写入文件，最多写满一个文件后返回
func (c *Compactor) write(path string, iter KeyIterator, throttle bool) (err error) {
	// 创建文件
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return errCompactionInProgress{err: err}
	}

	// syncingWriter 在原有io.Writer接口上扩展了刷盘接口
	type syncingWriter interface {
		io.Writer
		Sync() error
	}

	// 初始化限流Writer
	var (
		w           TSMWriter
		limitWriter syncingWriter = fd
	)
	if c.RateLimit != nil && throttle {
		limitWriter = limiter.NewWriterWithRate(fd, c.RateLimit)
	}
	w, err = NewTSMWriter(limitWriter)
	if err != nil {
		return err
	}

	// 关闭文件及错误检查，并根据错误类型判断是否需要移除文件
	defer func() {
		closeErr := w.Close()
		if err == nil {
			err = closeErr
		}
		_, inProgress := err.(errCompactionInProgress)
		maxBlocks := err == ErrMaxBlocksExceeded
		maxFileSize := err == errMaxFileExceeded
		if inProgress || maxBlocks || maxFileSize {
			return
		}
		if err != nil {
			w.Remove()
		}
	}()

	for iter.Next() {
		// 判断当前工作状态
		c.mu.RLock()
		enabled := c.snapshotsEnabled || c.compactionsEnabled
		c.mu.RUnlock()
		if !enabled {
			return errCompactionAborted{}
		}

		// 读取一个完整block的数据，或读取一个key的所有数据
		key, minTime, maxTime, block, err := iter.Read()
		if err != nil {
			return err
		}
		if minTime > maxTime {
			return fmt.Errorf("invalid index entry for block. min=%d, max=%d", minTime, maxTime)
		}

		// 把一个block的数据写入TSM文件
		if err := w.WriteBlock(key, minTime, maxTime, block); err == ErrMaxBlocksExceeded {
			// 数据写满后返回ErrMaxBlocksExceeded，此时补入Index区
			if err := w.WriteIndex(); err != nil {
				return err
			}
			return err
		} else if err != nil {
			return err
		}

		// 如果文件写满了，则补入Index区，结束文件，然后返回errMaxFileExceeded使得切换到下一个文件
		if w.Size() > maxTSMFileSize {
			if err := w.WriteIndex(); err != nil {
				return err
			}
			return errMaxFileExceeded
		}
	}

	// 判断迭代器中是否有出现错误，进而是否所有数据都被正确读取了
	if err := iter.Err(); err != nil {
		return err
	}

	// 所有数据写完了，补入Index区
	if err := w.WriteIndex(); err != nil {
		return err
	}
	return nil
}
