package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := loadConfig()
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
	if err := ensureKafkaTopics(ctx, cfg); err != nil {
		cfg.Logger.Fatalf("kafka ensure topics error: %v", err)
	}

	producer := newKafkaProducer(cfg)
	defer producer.Close(ctx)

	// Cliente MQTT + reconexão com backoff (bloqueia até cancel)
	client := buildMQTTClient(cfg, producer)
	connectWithBackoff(ctx, cfg, client, 2*time.Second, 30*time.Second)

	<-ctx.Done()
	cfg.Logger.Println("collector stopped")
}
