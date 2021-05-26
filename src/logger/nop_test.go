package logger

import "testing"

func TestNopLogger_ALL(t *testing.T) {
	logger := NewNop()
	logger.Debug("Debug Test")
	logger.Release("Release Test")
	logger.Error("Error Test")
}
