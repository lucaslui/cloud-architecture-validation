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
