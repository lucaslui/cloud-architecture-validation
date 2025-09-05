package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lucaslui/hems/collector/internal/kafka"
	"github.com/lucaslui/hems/collector/internal/mqtt"
	"github.com/lucaslui/hems/collector/internal/config"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Captura SIGINT/SIGTERM para shutdown gracioso
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		cfg.Logger.Printf("\nreceived signal: %v — shutting down...", s)
		cancel()
	}()

	if err := kafka.EnsureKafkaTopics(ctx, cfg); err != nil {
		cfg.Logger.Fatalf("kafka ensure topics error: %v", err)
	}

	producer := kafka.NewKafkaProducer(cfg)
	defer producer.Close(ctx)

	// Cliente MQTT + reconexão com backoff (bloqueia até cancel)
	client := mqtt.BuildMQTTClient(cfg, producer)
	mqtt.ConnectWithBackoff(ctx, cfg, client, 2*time.Second, 30*time.Second)

	<-ctx.Done()
	cfg.Logger.Println("collector stopped")
}
