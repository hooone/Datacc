package dlog

import "fmt"

func NewNop() Logger {
	return &NopLogger{}
}

type NopLogger struct {
}

func (c *NopLogger) Debug(str string) {
	fmt.Println(str)
}
func (c *NopLogger) Release(str string) {
	fmt.Println(str)
}
func (c *NopLogger) Error(str string) {
	fmt.Println(str)
}
