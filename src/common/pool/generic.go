package pool

// 可用对象池
type Generic struct {
	// 对象占用队列，实现等待功能
	pool chan interface{}
	// 对象构造方法
	fn func(sz int) interface{}
}

func NewGeneric(max int, fn func(sz int) interface{}) *Generic {
	return &Generic{
		pool: make(chan interface{}, max),
		fn:   fn,
	}
}

// 获得可用对象。无可用对象时等待
func (p *Generic) Get(sz int) interface{} {
	var c interface{}
	select {
	case c = <-p.pool:
	default:
		c = p.fn(sz)
	}

	return c
}

// 释放可用对象
func (p *Generic) Put(c interface{}) {
	select {
	case p.pool <- c:
	default:
	}
}
