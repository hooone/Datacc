package cache

import (
	"os"
	"strconv"

	"github.com/hooone/datacc/dlog"
	"github.com/hooone/datacc/store/wal"
)

type CacheLoader struct {
	files []string

	Logger dlog.Logger
}

func NewCacheLoader(files []string) *CacheLoader {
	return &CacheLoader{
		files:  files,
		Logger: dlog.NewNop(),
	}
}
func (cl *CacheLoader) Load(cache *Cache) error {
	var r *wal.WALSegmentReader
	// 遍历要加载的文件
	for _, fn := range cl.files {
		if err := func() error {
			// 打开文件
			f, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			defer f.Close()

			// 把文件信息写入log
			stat, err := os.Stat(f.Name())
			if err != nil {
				return err
			}
			cl.Logger.Release("Reading file " + f.Name() + ",size: " + strconv.Itoa(int(stat.Size())))

			// Nothing to read, skip it
			if stat.Size() == 0 {
				return nil
			}

			// 用字节流初始化reader
			if r == nil {
				r = wal.NewWALSegmentReader(f)
				defer r.Close()
			} else {
				r.Reset(f)
			}

			// 遍历读取WAL数据库
			for r.Next() {
				entry, err := r.Read()
				if err != nil {
					n := r.Count()
					cl.Logger.Release("File corrupt: " + f.Name())
					if err := f.Truncate(n); err != nil {
						return err
					}
					// WAL文件出错时，丢弃该文件
					break
				}

				switch t := entry.(type) {
				case *wal.WriteWALEntry:
					if err := cache.WriteMulti(t.Values); err != nil {
						return err
					}
				}
			}

			return r.Close()
		}(); err != nil {
			return err
		}
	}
	return nil
}
