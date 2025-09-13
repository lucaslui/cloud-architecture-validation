// cmd/main.go (ou internal/main.go, conforme seu layout)
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

	"github.com/lucaslui/hems/enricher/internal/broker"
	"github.com/lucaslui/hems/enricher/internal/config"
	"github.com/lucaslui/hems/enricher/internal/data"
	"github.com/lucaslui/hems/enricher/internal/processing"
	kafkasdk "github.com/segmentio/kafka-go"
)

func main() {
	logger := config.GetLogger()

	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("[boot] configuração inválida: %v", err)
	}

	logger.Printf("[info] enricher configs loaded:%s", cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := broker.EnsureKafkaTopics(ctx, cfg, logger); err != nil {
		logger.Fatalf("[error] kafka ensure topics error: %v", err)
	}

	kafkaClient := broker.NewKafkaClient(cfg)
	defer kafkaClient.Close()
	
	store, _ := data.LoadContext(cfg.ContextStorePath)

	proc := processing.NewProcessor(cfg, store, kafkaClient.MainProducer, kafkaClient.DLQProducer)

	// pipeline: fetch -> workers(proc) -> commit(batch)
	msgCh := make(chan kafkasdk.Message, 5000)
	ackCh := make(chan kafkasdk.Message, 5000)

	// workers
	var wg sync.WaitGroup
	wg.Add(cfg.ProcessingWorkers)
	for i := 0; i < cfg.ProcessingWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for m := range msgCh {
				lat, _ := proc.Process(ctx, m)
				if lat > 0 {
					// log.Printf("[out] key=%q latency_ms=%.2f", string(m.Key), float64(lat.Milliseconds()))
				}
				ackCh <- m // só ack após processar/publicar
			}
		}(i)
	}

	// committer (batch)
	doneCommit := make(chan struct{})
	go func() {
		defer close(doneCommit)
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

	// fetch loop
fetchLoop:
	for {
		m, err := kafkaClient.Consumer.FetchMessage(ctx)
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
		msgCh <- m
	}

	// shutdown ordenado
	close(msgCh)
	wg.Wait()
	close(ackCh)
	<-doneCommit
}
