package lsm

// 数据迭代器(Reader)接口
type KeyIterator interface {
	// 切换到下一个数据块
	Next() bool

	// 读取数据块
	Read() (key uint32, minTime int64, maxTime int64, data []byte, err error)

	// 读取完成释放资源
	Close() error

	// 读取过程中的Error
	Err() error
}
