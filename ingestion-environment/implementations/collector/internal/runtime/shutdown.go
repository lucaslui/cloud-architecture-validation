package runtime

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func SetupGracefulShutdown(cancel context.CancelFunc, logger *log.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		logger.Printf("\nreceived signal: %v — shutting down...", s)
		cancel()
	}()
}