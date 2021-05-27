package cache

import "sync"

type entry struct {
	mu     sync.RWMutex
	values values
}

func newEntryValues(values []value) (*entry, error) {
	e := &entry{}
	e.values = make([]value, 0, len(values))
	e.values = append(e.values, values...)

	// No values, don't check types and ordering
	if len(values) == 0 {
		return e, nil
	}

	return e, nil
}

func (e *entry) add(values []value) error {
	if len(values) == 0 {
		return nil
	}

	e.mu.Lock()
	if len(e.values) == 0 {
		e.values = values
		e.mu.Unlock()
		return nil
	}

	e.values = append(e.values, values...)
	e.mu.Unlock()
	return nil
}

// 加锁调用去重
func (e *entry) deduplicate() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.values) <= 1 {
		return
	}
	e.values = e.values.Deduplicate()
}
