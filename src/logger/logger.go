package logger

type Logger interface {
	Debug(string)
	Release(string)
	Error(string)
}
