package cache

import "sort"

type value struct {
	UnixNano int64
	Value    byte
}

type values []value

func (a values) Len() int           { return len(a) }
func (a values) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a values) Less(i, j int) bool { return a[i].UnixNano < a[j].UnixNano }

// 排序去重算法
func (a values) Deduplicate() values {
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
