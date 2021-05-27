package cache

import "testing"

// 写入和读取测试
func TestCache_Write(t *testing.T) {
	cache := NewCache(100)
	ts := make([]int64, 10)
	data := make([]byte, 10)
	for i := 0; i < len(ts); i++ {
		ts[i] = int64(i)
		data[i] = byte(i + 5)
	}
	// 写数据
	err := cache.Write(1, ts, data)
	if err != nil {
		t.Fatalf("write cache fail: %v", err)
	}
	err = cache.Write(2, ts, data)
	if err != nil {
		t.Fatalf("write append cache fail: %v", err)
	}
	// 读数据
	vls2 := cache.Values(2)
	if len(vls2) != 10 {
		t.Fatalf("key 2 values read error")
	}
	// 校验
	for i := 0; i < len(ts); i++ {
		if vls2[i].UnixNano != int64(i) {
			t.Fatalf("key 2 timestamp error. index: %d", i)
		}
		if vls2[i].Value != byte(i+5) {
			t.Fatalf("key 2 value error. index: %d", i)
		}
	}
}

// 快照测试
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

	// 数据快照
	_, err = cache.Snapshot()
	if err != nil {
		t.Fatalf("cache snapshot fail: %v", err)
	}

	// 数据写入
	for i := 0; i < len(ts); i++ {
		ts[i] = int64(i + 20)
		data[i] = byte(i + 25)
	}
	err = cache.Write(1, ts, data)
	if err != nil {
		t.Fatalf("write cache fail: %v", err)
	}

	// 读数据
	vls1 := cache.Values(1)
	if len(vls1) != 20 {
		t.Fatalf("key 1 values read error")
	}
	// 校验
	for i := 0; i < len(ts); i++ {
		if vls1[i].UnixNano != int64(i) {
			t.Fatalf("key 1 timestamp error. index: %d", i)
		}
		if vls1[i].Value != byte(i+5) {
			t.Fatalf("key 1 value error. index: %d", i)
		}
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
	err = cache.Write(1, ts, data)
	if err != nil {
		t.Fatalf("write append cache fail: %v", err)
	}

	// 数据去重
	cache.Deduplicate()

	// 读数据
	vls1 := cache.Values(1)
	if len(vls1) != 10 {
		t.Fatalf("key 1 values read error")
	}
	// 校验
	for i := 0; i < len(ts); i++ {
		if vls1[i].UnixNano != int64(i) {
			t.Fatalf("key 1 timestamp error. index: %d", i)
		}
		if vls1[i].Value != byte(i+5) {
			t.Fatalf("key 1 value error. index: %d", i)
		}
	}
}
