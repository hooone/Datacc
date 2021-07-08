package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hooone/datacc/common/pool"
	"github.com/hooone/datacc/store/coder"

	"github.com/golang/snappy"
)

const (
	// 单个WAL文件的大小限制为10MB
	DefaultSegmentSize = 10 * 1024 * 1024

	// 需要缓存到pool以供重用的byte切片的最大size
	walEncodeBufSize = 4 * 1024 * 1024

	// WAL文件前缀
	WALFilePrefix = "_"

	// WAL文件后缀
	WALFileExtension = "wal"
)

var (
	bytesPool = pool.NewLimitedBytes(256, walEncodeBufSize*2)
)

type WAL struct {
	// WAL文件路径
	path string

	// 当前写入的WAL文件的序列号
	currentSegmentID int
	//  当前写入的WAL文件的对象封装
	currentSegmentWriter WALSegmentWriter

	// 状态统计
	stats *WALStatistics
	// 最后写入时间
	lastWriteTime time.Time
	// 写入并发锁
	mu sync.RWMutex
	// 用于优雅关闭的通道
	closing chan struct{}
	// 异步刷盘互锁标志位，刷盘定时器的启动状态
	syncCount uint64
	// 刷盘延时
	syncDelay time.Duration
	// 用于传递刷写硬盘的结果，把结果传递给所有在等待的协程
	syncWaiters chan chan error
}

func NewWAL(path string) *WAL {
	return &WAL{
		path: path,

		closing:     make(chan struct{}),
		syncWaiters: make(chan chan error, 1024),
		stats:       &WALStatistics{},
	}
}

// 把数据写入WAL并计数
func (l *WAL) WriteMulti(values map[uint32][]coder.Value) (int, error) {
	entry := &WriteWALEntry{
		Values: values,
	}

	id, err := l.writeToLog(entry)
	if err != nil {
		atomic.AddInt64(&l.stats.WriteErr, 1)
		return -1, err
	}
	atomic.AddInt64(&l.stats.WriteOK, 1)

	return id, nil
}

// 把数据写入WAL，并等待定时延迟刷盘成功
func (l *WAL) writeToLog(entry WALEntry) (int, error) {
	// 从池中获取byte buffer用于编码
	bytes := bytesPool.Get(entry.MarshalSize())

	// 将entry编码
	b, err := entry.Encode(bytes)
	if err != nil {
		bytesPool.Put(bytes)
		return -1, err
	}

	// 从池中获取byte buffer用于压缩
	encBuf := bytesPool.Get(snappy.MaxEncodedLen(len(b)))
	// 压缩
	compressed := snappy.Encode(encBuf, b)
	// 归还编码buffer
	bytesPool.Put(bytes)

	// 用于收集协程中产生的error
	syncErr := make(chan error)

	segID, err := func() (int, error) {
		l.mu.Lock()
		defer l.mu.Unlock()

		// 检查优雅退出
		select {
		case <-l.closing:
			return -1, ErrWALClosed
		default:
		}

		// 检查是否需要切换到下一个文件
		if err := l.rollSegment(); err != nil {
			return -1, fmt.Errorf("error rolling WAL segment: %v", err)
		}

		// 将数据写入文件
		if err := l.currentSegmentWriter.Write(compressed); err != nil {
			return -1, fmt.Errorf("error writing WAL entry: %v", err)
		}

		// 将error收集器给入收集器通道
		select {
		case l.syncWaiters <- syncErr:
		default:
			return -1, fmt.Errorf("error syncing wal")
		}

		// 定时调用异步刷盘
		l.scheduleSync()

		// 统计当前文件的写入数量
		atomic.StoreInt64(&l.stats.CurrentBytes, int64(l.currentSegmentWriter.getSize()))
		l.lastWriteTime = time.Now().UTC()

		return l.currentSegmentID, nil
	}()

	// 归还压缩buffer
	bytesPool.Put(encBuf)

	if err != nil {
		return segID, err
	}

	// 等待刷盘结果
	return segID, <-syncErr
}

// 创建新的WAL文件
func (l *WAL) newSegmentFile() error {
	l.currentSegmentID++

	// 如果已有正在使用的文件，刷盘关闭释放，统计大小
	if l.currentSegmentWriter != nil {
		l.sync()
		if err := l.currentSegmentWriter.close(); err != nil {
			return err
		}
		atomic.StoreInt64(&l.stats.OldBytes, int64(l.currentSegmentWriter.getSize()))
	}

	// 新建文件并打开
	fileName := filepath.Join(l.path, fmt.Sprintf("%s%05d.%s", WALFilePrefix, l.currentSegmentID, WALFileExtension))
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	l.currentSegmentWriter = NewWALSegmentWriter(fd)

	// 重置当前文件的写入数量统计
	atomic.StoreInt64(&l.stats.CurrentBytes, 0)

	return nil
}

// 检查是否需要切换到下一个文件
func (l *WAL) rollSegment() error {
	if l.currentSegmentWriter == nil || l.currentSegmentWriter.getSize() > DefaultSegmentSize {
		if err := l.newSegmentFile(); err != nil {
			return fmt.Errorf("error opening new segment file for wal (2): %v", err)
		}
		return nil
	}

	return nil
}

// 刷盘并反馈结果
func (l *WAL) sync() {
	err := l.currentSegmentWriter.sync()
	for len(l.syncWaiters) > 0 {
		errC := <-l.syncWaiters
		errC <- err
	}
}

// 定时延迟刷盘
func (l *WAL) scheduleSync() {
	// 刷盘互锁，其他协程正在刷盘则认为本协程也刷盘成功
	if !atomic.CompareAndSwapUint64(&l.syncCount, 0, 1) {
		return
	}

	// 定时延迟刷盘并将结果反馈给每一个调用刷盘接口的协程
	go func() {
		var timerCh <-chan time.Time

		// 如果不需要延迟，使用已经关闭的通道实现
		if l.syncDelay == 0 {
			timerChrw := make(chan time.Time)
			close(timerChrw)
			timerCh = timerChrw
		} else {
			// 定时器
			t := time.NewTicker(l.syncDelay)
			defer t.Stop()
			timerCh = t.C
		}

		// 循环等待定时器触发或优雅退出
		for {
			select {
			// 刷盘定时器触发
			case <-timerCh:
				l.mu.Lock()
				if len(l.syncWaiters) == 0 {
					atomic.StoreUint64(&l.syncCount, 0)
					l.mu.Unlock()
					return
				}

				l.sync()
				l.mu.Unlock()
			// 优雅退出
			case <-l.closing:
				atomic.StoreUint64(&l.syncCount, 0)
				return
			}
		}
	}()
}
