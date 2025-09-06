package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	cfgpkg "github.com/lucaslui/hems/collector/internal/config"
	kafkapkg "github.com/lucaslui/hems/collector/internal/kafka"
	mqttpkg "github.com/lucaslui/hems/collector/internal/mqtt"
)

func main() {
	cfg, err := cfgpkg.LoadConfig()
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

	// Garante tópicos Kafka antes de criar produtores
	if err := kafkapkg.EnsureKafkaTopics(ctx, cfg); err != nil {
		cfg.Logger.Fatalf("kafka ensure topics error: %v", err)
	}

	producer := kafkapkg.NewKafkaProducer(cfg)
	defer producer.Close(ctx)

	// Cliente MQTT + reconexão com backoff (bloqueia até cancel)
	client := mqttpkg.BuildMQTTClient(cfg, producer)
	mqttpkg.ConnectWithBackoff(ctx, cfg, client, 2*time.Second, 30*time.Second)

	<-ctx.Done()
	cfg.Logger.Println("collector stopped")
}
