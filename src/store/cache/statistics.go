package cache

// CacheStatistics 工作状态统计
type CacheStatistics struct {
	// 当前Cache占内存的总大小
	MemSizeBytes int64

	// 写入成功计数
	WriteOK int64
	// 写入失败计数
	WriteErr int64
}
