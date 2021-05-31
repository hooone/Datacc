package limiter

import (
	"context"
	"io"
	"os"
)

type Rate interface {
	WaitN(ctx context.Context, n int) error
	Burst() int
}

// 对硬盘进行限流写入。
// 限流原理是初始化一个限制每秒写入数量的桶，然后从桶中抢到资源后才能写入
type Writer struct {
	w       io.WriteCloser
	limiter Rate
	ctx     context.Context
}

func NewWriterWithRate(w io.WriteCloser, limiter Rate) *Writer {
	return &Writer{
		w:       w,
		ctx:     context.Background(),
		limiter: limiter,
	}
}

// 对硬盘进行限流写入
func (s *Writer) Write(b []byte) (int, error) {
	if s.limiter == nil {
		return s.w.Write(b)
	}

	var n int
	for n < len(b) {
		// 计算单次能够写入的数量
		wantToWriteN := len(b[n:])
		if wantToWriteN > s.limiter.Burst() {
			wantToWriteN = s.limiter.Burst()
		}

		// 写入数据
		wroteN, err := s.w.Write(b[n : n+wantToWriteN])
		if err != nil {
			return n, err
		}

		// 统计实际写入是数量
		n += wroteN

		// 等待一段时间，直到允许写入的数量恢复
		if err := s.limiter.WaitN(s.ctx, wroteN); err != nil {
			return n, err
		}
	}

	return n, nil
}

func (s *Writer) Sync() error {
	if f, ok := s.w.(*os.File); ok {
		return f.Sync()
	}
	return nil
}

func (s *Writer) Name() string {
	if f, ok := s.w.(*os.File); ok {
		return f.Name()
	}
	return ""
}

func (s *Writer) Close() error {
	return s.w.Close()
}
