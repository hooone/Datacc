package wal

import (
	"datacc/store/coder"
	"fmt"
	"io/ioutil"
	"testing"
)

func TesWAL_Write(t *testing.T) {
	dir := MustTempDir()
	wal := NewWAL(dir)
	var values map[uint32][]coder.Value
	v1 := make([]coder.Value, 10)
	v2 := make([]coder.Value, 10)
	for i := 0; i < 10; i++ {
		v1[i] = coder.NewValue(int64(i)+11, byte(i)+23)
		v2[i] = coder.NewValue(int64(i)+17, byte(i)+29)
	}
	values[1] = v1
	values[2] = v2
	_, err := wal.WriteMulti(values)
	if err != nil {
		t.Fatalf("write WAL fail: %v", err)
	}
}

func MustTempDir() string {
	dir, err := ioutil.TempDir("C:\\share\\tes", "tsm1-")
	if err != nil {
		panic(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	return dir
}
