package lsm

import (
	"datacc/store/cache"
	"fmt"
	"io/ioutil"
	"testing"
)

func TestCompact_WriteSnapshot(t *testing.T) {
	dir := MustTempDir()
	// defer os.RemoveAll(dir)

	// 模拟数据
	c := cache.NewCache(100)
	ts := make([]int64, 10)
	data := make([]byte, 10)
	for i := 1; i < len(ts); i++ {
		ts[i] = int64(i*10 + 10)
		data[i] = byte(i*10 + 16)
	}
	// 写数据
	err := c.Write(1, ts, data)
	if err != nil {
		t.Fatalf("write cache fail: %v", err)
	}
	err = c.Write(2, ts, data)
	if err != nil {
		t.Fatalf("write append cache fail: %v", err)
	}

	compactor := NewCompactor()
	compactor.Dir = dir
	compactor.FileStore = &fakeFileStore{}
	compactor.Open()

	files, err := compactor.WriteSnapshot(c)
	if err != nil {
		t.Fatalf("unexpected error writing snapshot: %v", err)
	}

	fmt.Println(files)
}

type fakeFileStore struct {
}

func (f *fakeFileStore) NextGeneration() int {
	return 1
}

func MustTempDir() string {
	dir, err := ioutil.TempDir("C:\\share\\tes", "tsm1-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}
