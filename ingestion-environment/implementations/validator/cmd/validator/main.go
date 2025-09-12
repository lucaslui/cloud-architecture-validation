package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lucaslui/hems/validator/internal/broker"
	"github.com/lucaslui/hems/validator/internal/config"
	"github.com/lucaslui/hems/validator/internal/processing"
	"github.com/lucaslui/hems/validator/internal/registry"
	kafkasdk "github.com/segmentio/kafka-go"
)

func main() {
	logger := config.GetLogger()

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("[boot] configuração inválida: %v", err)
	}

	log.Printf("[info] validator configs loaded:%s", cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := broker.EnsureKafkaTopics(ctx, cfg, logger); err != nil {
		logger.Fatalf("[error] kafka ensure topics error: %v", err)
	}

	// 2) construir reader/writers
	kafkaClient := broker.NewKafkaClient(cfg)

	// 3) registry + processor
	reg := registry.NewRedisRegistry(registry.RedisOpts{
		Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB,
		Namespace: cfg.RedisNamespace, InvalidateChannel: cfg.RedisInvalidateChan,
		UsePubSub: cfg.RedisUsePubSub, Timeout: 5 * time.Second,
	})
	proc := processing.NewProcessor(cfg, reg, kafkaClient.MainProducer, kafkaClient.DQLProducer)

	// 4) pipeline: fetch -> workers(proc) -> commits(batch)
	msgCh := make(chan kafkasdk.Message, 5000)
	ackCh := make(chan kafkasdk.Message, 5000)

	// 4.1 workers
	var wg sync.WaitGroup
	wg.Add(cfg.ProcessingWorkers)
	for i := 0; i < cfg.ProcessingWorkers; i++ {
		go func() {
			defer wg.Done()
			for msg := range msgCh {
				lat, _ := proc.Process(ctx, msg)
				if lat > 0 {
					// logs leves (evite spam por msg em produção)
					// log.Printf("[out] key=%q latency_ms=%.2f", string(m.Key), float64(lat.Milliseconds()))
				}
				ackCh <- msg // só ack depois de processar/publicar
			}
		}()
	}

	// 4.2 committer
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		batch := make([]kafkasdk.Message, 0, cfg.KafkaAckBatchSize)

		flush := func() {
			if len(batch) == 0 {
				return
			}
			_ = kafkaClient.Consumer.CommitMessages(context.Background(), batch...)
			batch = batch[:0]
		}

		for {
			select {
			case m, ok := <-ackCh:
				if !ok {
					flush()
					return
				}
				batch = append(batch, m)
				if len(batch) >= cfg.KafkaAckBatchSize {
					flush()
				}
			case <-t.C:
				flush()
			}
		}
	}()

	// 4.3 fetch loop
fetchLoop:
	for {
		msg, err := kafkaClient.Consumer.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("[shutdown] encerrando consumo")
				break fetchLoop
			}
			log.Printf("[error] leitura Kafka: %v", err)
			continue
		}
		// log.Printf("[in] partition=%d offset=%d key=%q size=%d ts=%s",
		// 	m.Partition, m.Offset, string(m.Key), len(m.Value), m.Time.UTC().Format(time.RFC3339))
		msgCh <- msg
	}

	// 5) shutdown ordenado
	close(msgCh)
	wg.Wait()
	close(ackCh)
}
