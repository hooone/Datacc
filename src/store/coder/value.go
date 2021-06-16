package coder

import "sort"

type Value struct {
	UnixNano int64
	Value    byte
}

type Values []Value

func (a Values) Len() int           { return len(a) }
func (a Values) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Values) Less(i, j int) bool { return a[i].UnixNano < a[j].UnixNano }

// NewValue returns a new value.
func NewValue(t int64, v byte) Value {
	return Value{UnixNano: t, Value: v}
}

// 排序去重算法
func (a Values) Deduplicate() Values {
	if len(a) <= 1 {
		return a
	}

	// 验证是否需要排序
	var needSort bool
	for i := 1; i < len(a); i++ {
		if a[i-1].UnixNano >= a[i].UnixNano {
			needSort = true
			break
		}
	}
	if !needSort {
		return a
	}

	// 排序
	sort.Stable(a)

	// 去重
	var i int
	for j := 1; j < len(a); j++ {
		v := a[j]
		if v.UnixNano != a[i].UnixNano {
			i++
		}
		a[i] = v

	}
	return a[:i+1]
}
