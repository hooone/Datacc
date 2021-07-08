package dlog

type Logger interface {
	Debug(string)
	Release(string)
	Error(string)
}
