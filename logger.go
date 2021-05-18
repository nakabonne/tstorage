package tstorage

import "log"

// Logger is a logging interface
type Logger interface {
	Printf(format string, v ...interface{})
}

// DefaultLogger returns a Logger implementation backed by stdlib's log
func DefaultLogger() Logger {
	return &log.Logger{}
}

type nopLogger struct{}

func (l *nopLogger) Printf(_ string, _ ...interface{}) {
	return
}
