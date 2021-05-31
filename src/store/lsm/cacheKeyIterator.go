package lsm

import (
	"datacc/store/cache"
	"runtime"
	"sync/atomic"
)

type cacheKeyIterator struct {
	// 要读取的cache
	cache *cache.Cache
	// 每次读取的数据量
	size int
	// cache中所有包含的key
	keys []uint32
	// 中断通道，用于优雅关闭
	interrupt chan struct{}

	// 读取完成的key计数
	i int
	// 每个key的读取完成通道
	ready []chan struct{}
	// 已编码压缩的数据缓存
	blocks [][]cacheBlock
	// 错误缓存
	err error
}

type cacheBlock struct {
	k                uint32
	minTime, maxTime int64
	b                []byte
	err              error
}

func NewCacheKeyIterator(cache *cache.Cache, size int, interrupt chan struct{}) KeyIterator {
	// 获得cache中的所有key
	keys := cache.Keys()

	// 每个key的读取完成通道，用于key切换
	chans := make([]chan struct{}, len(keys))
	for i := 0; i < len(keys); i++ {
		chans[i] = make(chan struct{}, 1)
	}

	cki := &cacheKeyIterator{
		i:         -1,
		size:      size,
		cache:     cache,
		keys:      keys,
		ready:     chans,
		blocks:    make([][]cacheBlock, len(keys)),
		interrupt: interrupt,
	}

	// 异步的数据编码压缩
	go cki.encode()

	return cki
}

// 切化到下一个数据块
func (c *cacheKeyIterator) Next() bool {
	// 判断当前key是否还有数据
	if c.i >= 0 && c.i < len(c.ready) && len(c.blocks[c.i]) > 0 {
		c.blocks[c.i] = c.blocks[c.i][1:]
		if len(c.blocks[c.i]) > 0 {
			return true
		}
	}

	// 切换key
	c.i++

	// 所有压缩完成的key都读取完成
	if c.i >= len(c.ready) {
		return false
	}

	// 等待下一个key编码压缩完成
	<-c.ready[c.i]
	return true
}

// 以编码后的数据块的格式读取数据
func (c *cacheKeyIterator) Read() (uint32, int64, int64, []byte, error) {
	// 状态检查，优雅退出
	select {
	case <-c.interrupt:
		return 0, 0, 0, nil, c.err
	default:
	}

	blk := c.blocks[c.i][0]
	return blk.k, blk.minTime, blk.maxTime, blk.b, blk.err
}

// 把cache中的所有数据压缩后保存到block中
func (c *cacheKeyIterator) encode() {
	concurrency := runtime.GOMAXPROCS(0)
	n := len(c.ready)

	// 给每个并发协程每次分配一个key
	chunkSize := 1
	idx := uint64(0)

	for cour := 0; cour < concurrency; cour++ {
		// 根据可用cpu数量，为每个cpu核创建协程
		go func() {
			// 从池中取出encoder，如果都被占用，则会等待释放
			tenc := getTimeEncoder(DefaultMaxPointsPerBlock)
			benc := getByteEncoder(DefaultMaxPointsPerBlock)

			defer putTimeEncoder(tenc)
			defer putByteEncoder(benc)

			for {
				// 原子方式获得下一个要处理的key的序号
				keyidx := int(atomic.AddUint64(&idx, uint64(chunkSize))) - chunkSize

				// 所有的key都处理完成或者正在被处理
				if keyidx >= n {
					break
				}

				// 从cache中获得数据
				key := c.keys[keyidx]
				values := c.cache.Values(key)

				for len(values) > 0 {
					// 根据指定的每个block大小，将数据拆分成为[:end]
					end := len(values)
					if end > c.size {
						end = c.size
					}

					// 获得起止时间
					minTime, maxTime := values[0].UnixNano, values[end-1].UnixNano
					var b []byte
					var err error

					// 根据tsm编码规则，将数据[]value转化为block
					b, err = encodeByteBlockUsing(nil, values[:end], tenc, benc)

					// 移除已经编码压缩的数据
					values = values[end:]

					// 保存已编码压缩完成的数据
					c.blocks[keyidx] = append(c.blocks[keyidx], cacheBlock{
						k:       key,
						minTime: minTime,
						maxTime: maxTime,
						b:       b,
						err:     err,
					})

					if err != nil {
						c.err = err
					}
				}
				// 发送key的编码压缩完成信号
				c.ready[keyidx] <- struct{}{}
			}
		}()
	}
}

func (c *cacheKeyIterator) Close() error {
	return nil
}

func (c *cacheKeyIterator) Err() error {
	return c.err
}

// 索引区的估算大小
func (c *cacheKeyIterator) EstimatedIndexSize() int {
	return len(c.keys) * 4
}
