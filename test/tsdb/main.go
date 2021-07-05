package main

import (
	"github.com/hooone/datacc/src/dlog"
)

func main() {
	logger := dlog.NewNop()
	logger.Debug("123")

}
