package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lucaslui/hems/enricher-validator/internal/config"
	"github.com/lucaslui/hems/enricher-validator/internal/data"
	"github.com/lucaslui/hems/enricher-validator/internal/kafka"
	"github.com/lucaslui/hems/enricher-validator/internal/processing"
	"github.com/lucaslui/hems/enricher-validator/internal/registry"
)

func main() {
	cfg := config.Load()

	log.Printf("[boot] enricher-validator | brokers=%v group=%s in=%s out=%s dlq=%s redis=%s ns=%s",
		cfg.Brokers, cfg.GroupID, cfg.InputTopic, cfg.OutputTopic, cfg.DLQTopic, cfg.RedisAddr, cfg.RedisNamespace)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	defer cancel()

	// 1) garantir tópicos
	ensureCtx, ensureCancel := context.WithTimeout(ctx, 15*time.Second)
	kafka.EnsureTopics(ensureCtx, kafka.EnsureTopicsArgs{
		Brokers: cfg.Brokers, InputTopic: cfg.InputTopic,
		OutputTopic: cfg.OutputTopic, OutTopicPartitions: cfg.OutTopicPartitions,
		DLQTopic: cfg.DLQTopic, DLQTopicPartitions: cfg.DLQTopicPartitions,
	})
	ensureCancel()

	// 2) construir reader/writers
	reader := kafka.NewReader(cfg)
	defer reader.Close()
	writerOut := kafka.NewWriter(cfg.Brokers, cfg.OutputTopic)
	defer writerOut.Close()
	writerDLQ := kafka.NewWriter(cfg.Brokers, cfg.DLQTopic)
	defer writerDLQ.Close()

	// 3) registry + contexto
	reg := registry.NewRedisRegistry(registry.RedisOpts{
		Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB,
		Namespace: cfg.RedisNamespace, InvalidateChannel: cfg.RedisInvalidateChan,
		UsePubSub: cfg.RedisUsePubSub, Timeout: 5 * time.Second,
	})
	store, _ := data.LoadContext(cfg.ContextStorePath)

	// 4) processor
	proc := processing.NewProcessor(cfg, reg, store, writerOut, writerDLQ)

	// 5) loop principal (obter → validar → enriquecer → publicar)
	for {
		msg, err := reader.ReadMessage(ctx)
		
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("[shutdown] encerrando consumo")
				return
			}
			log.Printf("[error] leitura Kafka: %v", err)
			continue
		}

		log.Printf("[in] topic=%s partition=%d offset=%d key=%q size=%d ts=%s",
			cfg.InputTopic, msg.Partition, msg.Offset, string(msg.Key), len(msg.Value), msg.Time.UTC().Format(time.RFC3339))

		lat, _ := proc.Process(ctx, msg)

		if lat > 0 {
			log.Printf("[out] topic=%s key=%q latency_ms=%.2f", cfg.OutputTopic, string(msg.Key), float64(lat.Milliseconds()))
		}
	}
}
