package wal

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/hooone/datacc/store/coder"

	"github.com/golang/snappy"
)

func TestWAL_Write(t *testing.T) {
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	wal := NewWAL(dir)
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
	_, err := wal.WriteMulti(values)
	if err != nil {
		t.Fatalf("write WAL fail: %v", err)
	}
}
func TestWAL_ReadWrite(t *testing.T) {
	// 准备文件
	dir := MustTempDir()
	defer os.RemoveAll(dir)
	f := MustTempFile(dir)
	w := NewWALSegmentWriter(f)

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
	entry := &WriteWALEntry{
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

	// WAL Reader
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek WAL fail: %v", err)
	}
	r := NewWALSegmentReader(f)
	if !r.Next() {
		t.Fatalf("expected next, got false")
	}
	we, err := r.Read()
	if err != nil {
		t.Fatalf("Read WAL Entry fail: %v", err)
	}
	e, ok := we.(*WriteWALEntry)
	if !ok {
		t.Fatalf("expected WriteWALEntry: got %#v", e)
	}

	// 读取出的数据校验
	for i := 0; i < 10; i++ {
		if e.Values[uint32(1)][i].UnixNano != int64(i)+11 {
			t.Fatalf("points mismatch 1: got %v, exp %v", e.Values[1][i].UnixNano, int64(i)+11)
		}
		if e.Values[uint32(1)][i].Value != byte(i)+23 {
			t.Fatalf("points mismatch 2: got %v, exp %v", e.Values[1][i].Value, byte(i)+23)
		}
		if e.Values[uint32(2)][i].UnixNano != int64(i)+17 {
			t.Fatalf("points mismatch 3: got %v, exp %v", e.Values[2][i].UnixNano, int64(i)+17)
		}
		if e.Values[uint32(2)][i].Value != byte(i)+29 {
			t.Fatalf("points mismatch 4: got %v, exp %v", e.Values[2][i].Value, byte(i)+29)
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
