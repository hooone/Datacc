package cache

import "sync"

type partition struct {
	// 写入锁
	mu sync.RWMutex

	// Key-Values字典
	store map[uint32]*entry
}

// 区块写入- 线程安全
func (p *partition) write(key uint32, values []value) (bool, error) {
	// 通过key获得entry
	p.mu.RLock()
	e := p.store[key]
	p.mu.RUnlock()

	// 在已有的entry中添加数据
	if e != nil {
		return false, e.add(values)
	}

	// key未存在，创建新的entry
	p.mu.Lock()
	defer p.mu.Unlock()

	// 再次确认当前key没有对应的entry
	if e = p.store[key]; e != nil {
		return false, e.add(values)
	}

	// 创建entry
	e, err := newEntryValues(values)
	if err != nil {
		return false, err
	}

	p.store[key] = e
	return true, nil
}
