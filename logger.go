package tstorage

import "log"

type Logger interface {
	Printf(format string, v ...interface{})
}

func DefaultLogger() Logger {
	return &log.Logger{}
}

type nopLogger struct{}

func (l *nopLogger) Printf(_ string, _ ...interface{}) {
	return
}
