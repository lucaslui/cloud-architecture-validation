package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lucaslui/hems/collector/internal/config"
	"github.com/lucaslui/hems/collector/internal/broker"
	"github.com/lucaslui/hems/collector/internal/mqtt"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupGracefulShutdown(cancel, cfg.Logger)

	if err := broker.EnsureKafkaTopics(ctx, cfg); err != nil {
		cfg.Logger.Fatalf("kafka ensure topics error: %v", err)
	}

	producer := broker.NewKafkaProducer(cfg)
	defer producer.Close(ctx)

	dispatcher := broker.NewKafkaDispatcher(producer, 10_000)
	defer dispatcher.Stop()

	client := mqtt.BuildMQTTClient(cfg, producer, dispatcher)
	mqtt.ConnectWithBackoff(ctx, cfg, client, 2*time.Second, 30*time.Second)

	<-ctx.Done()
	cfg.Logger.Println("collector stopped")
}

func setupGracefulShutdown(cancel context.CancelFunc, logger *log.Logger) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		logger.Printf("\nreceived signal: %v — shutting down...", s)
		cancel()
	}()
}
