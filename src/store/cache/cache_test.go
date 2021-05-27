package cache

import "testing"

func TestCache_Write(t *testing.T) {
	cache := NewCache(100)
	ts := make([]int64, 10)
	data := make([]byte, 10)
	for i := 0; i < len(ts); i++ {
		ts[i] = int64(i)
		data[i] = byte(i + 5)
	}
	err := cache.Write(1, ts, data)
	if err != nil {
		t.Fatalf("write cache fail: %v", err)
	}
	err = cache.Write(2, ts, data)
	if err != nil {
		t.Fatalf("write append cache fail: %v", err)
	}
}

// 去重测试
func TestCache_Deduplicate(t *testing.T) {
	cache := NewCache(100)
	ts := make([]int64, 10)
	data := make([]byte, 10)
	for i := 0; i < len(ts); i++ {
		ts[i] = int64(i)
		data[i] = byte(i + 5)
	}

	// 数据写入
	err := cache.Write(1, ts, data)
	if err != nil {
		t.Fatalf("write cache fail: %v", err)
	}
	err = cache.Write(2, ts, data)
	if err != nil {
		t.Fatalf("write append cache fail: %v", err)
	}

	// 数据去重
	cache.Deduplicate()
}

func TestCache_Snapshot(t *testing.T) {
	cache := NewCache(100)
	ts := make([]int64, 10)
	data := make([]byte, 10)
	for i := 0; i < len(ts); i++ {
		ts[i] = int64(i)
		data[i] = byte(i + 5)
	}

	// 数据写入
	err := cache.Write(1, ts, data)
	if err != nil {
		t.Fatalf("write cache fail: %v", err)
	}
	err = cache.Write(2, ts, data)
	if err != nil {
		t.Fatalf("write append cache fail: %v", err)
	}

	// 数据快照
	_, err = cache.Snapshot()
	if err != nil {
		t.Fatalf("cache snapshot fail: %v", err)
	}
}
