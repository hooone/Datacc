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
	cache.Write(1, ts, data)
	cache.Write(2, ts, data)
}
