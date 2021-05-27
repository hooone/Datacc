package lsm

// 数据迭代器(Reader)接口
type KeyIterator interface {
	// 切换到下一个数据块
	Next() bool

	// 读取数据块
	Read() (key []byte, minTime int64, maxTime int64, data []byte, err error)

	// 读取完成释放资源
	Close() error

	// 读取过程中的Error
	Err() error

	// 估算的索引Size。
	// 因为索引在TSM文件的末尾，如果索引小，就存在缓存，在所有数据写完后再把索引写入文件
	// 如果索引大，则把索引单独写临时文件
	EstimatedIndexSize() int
}
