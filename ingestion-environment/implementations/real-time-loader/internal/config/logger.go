package config

import (
	"log"
	"os"
	"sync"
)

var (
	logger     *log.Logger
	initLogger sync.Once
)

func GetLogger() *log.Logger {
	initLogger.Do(func() {
		logger = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	})
	return logger
}

func Truncate(b []byte, max int) string {
	if len(b) <= max {
		return string(b)
	}
	return string(b[:max]) + "..."
}
