package cache

import (
	"datacc/store/coder"
	"datacc/store/wal"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/golang/snappy"
)

func TestCacheLoader_LoadSingle(t *testing.T) {
	// 准备文件
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := wal.NewWALSegmentWriter(f)

	// 准备数据
	v1 := make([]coder.Value, 10)
	v2 := make([]coder.Value, 10)
	for i := 0; i < 10; i++ {
		v1[i] = coder.NewValue(int64(i)+11, byte(i)+23)
		v2[i] = coder.NewValue(int64(i)+17, byte(i)+29)
	}
	values := map[uint32][]coder.Value{
		1: v1,
		2: v2,
	}

	// 编码并压缩
	entry := &wal.WriteWALEntry{
		Values: values,
	}
	bytes := make([]byte, 1024<<2)
	b, err := entry.Encode(bytes)
	if err != nil {
		panic(fmt.Sprintf("error encoding: %v", err))
	}
	b = snappy.Encode(b, b)

	// 写入数据
	err = w.Write(b)
	if err != nil {
		t.Fatalf("write WAL fail: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush WAL fail: %v", err)
	}

	// 读取WAL到Cache.
	cache := NewCache(1024)
	loader := NewCacheLoader([]string{f.Name()})
	if err := loader.Load(cache); err != nil {
		t.Fatalf("failed to load cache: %s", err.Error())
	}

	// 检查cache中的数据
	v1Read := cache.Values(1)
	v2Read := cache.Values(2)
	for i := 0; i < 10; i++ {
		if v1Read[i].UnixNano != int64(i)+11 {
			t.Fatalf("points mismatch 1: got %v, exp %v", v1Read[i].UnixNano, int64(i)+11)
		}
		if v1Read[i].Value != byte(i)+23 {
			t.Fatalf("points mismatch 2: got %v, exp %v", v1Read[i].Value, byte(i)+23)
		}
		if v2Read[i].UnixNano != int64(i)+17 {
			t.Fatalf("points mismatch 3: got %v, exp %v", v2Read[i].UnixNano, int64(i)+17)
		}
		if v2Read[i].Value != byte(i)+29 {
			t.Fatalf("points mismatch 4: got %v, exp %v", v2Read[i].Value, byte(i)+29)
		}
	}
}

func MustTempDir() string {
	dir, err := ioutil.TempDir("C:\\share\\tes", "tsm1-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}

func MustTempFile(dir string) *os.File {
	f, err := ioutil.TempFile(dir, "tsm1test")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp file: %v", err))
	}
	return f
}
