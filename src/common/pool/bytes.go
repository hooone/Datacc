package pool

// byte缓存池
type LimitedBytes struct {
	maxSize int
	pool    chan []byte
}

// 新建byte缓存池
func NewLimitedBytes(capacity int, maxSize int) *LimitedBytes {
	return &LimitedBytes{
		pool:    make(chan []byte, capacity),
		maxSize: maxSize,
	}
}

// 从池中获取byte buffer
func (p *LimitedBytes) Get(sz int) []byte {
	var c []byte
	select {
	// 1. 从池中取出byte切片
	case c = <-p.pool:
	default:
		// 2. 如果池是空的，则直接make
		return make([]byte, sz)
	}
	// 3. 如果取出的byte切片不够大，则直接make，之前取出的将会被丢弃
	if cap(c) < sz {
		return make([]byte, sz)
	}
	return c[:sz]
}

// 将使用过的buffer还给缓存池
func (p *LimitedBytes) Put(c []byte) {
	// 丢弃释放过大的buffer，不再重用
	if cap(c) >= p.maxSize {
		return
	}

	// 将使用过的buffer存入pool，可供再次使用，最多存capacity个切片
	select {
	case p.pool <- c:
	default:
	}
}
