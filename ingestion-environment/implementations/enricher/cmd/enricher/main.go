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

	"github.com/lucaslui/hems/enricher-validator/internal/config"
	"github.com/lucaslui/hems/enricher-validator/internal/data"
	"github.com/lucaslui/hems/enricher-validator/internal/broker"
	"github.com/lucaslui/hems/enricher-validator/internal/processing"
	kafkasdk "github.com/segmentio/kafka-go"
)

func main() {
	cfg := config.Load()

	log.Printf("[boot] enricher | brokers=%v group=%s in=%s out=%s dlq=%s",
		cfg.Brokers, cfg.GroupID, cfg.InputTopic, cfg.OutputTopic, cfg.DLQTopic)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1) garantir tópicos
	ensureCtx, ensureCancel := context.WithTimeout(ctx, 15*time.Second)
	broker.EnsureTopics(ensureCtx, broker.EnsureTopicsArgs{
		Brokers: cfg.Brokers, InputTopic: cfg.InputTopic,
		OutputTopic: cfg.OutputTopic, OutTopicPartitions: cfg.OutTopicPartitions,
		DLQTopic: cfg.DLQTopic, DLQTopicPartitions: cfg.DLQTopicPartitions,
	})
	ensureCancel()

	// 2) construir reader/writers
	reader := broker.NewReader(cfg)
	defer reader.Close()
	writerOut := broker.NewWriter(cfg.Brokers, cfg.OutputTopic)
	defer writerOut.Close()
	writerDLQ := broker.NewWriter(cfg.Brokers, cfg.DLQTopic)
	defer writerDLQ.Close()

	// 3) carregar contexto de enriquecimento
	store, _ := data.LoadContext(cfg.ContextStorePath)

	// 4) processor
	proc := processing.NewProcessor(cfg, store, writerOut, writerDLQ)

	// 5) pipeline: fetch -> workers(proc) -> commit(batch)
	const (
		workers      = 8
		ackBatchSize = 500
	)
	msgCh := make(chan kafkasdk.Message, 5000)
	ackCh := make(chan kafkasdk.Message, 5000)

	// 5.1 workers
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			for m := range msgCh {
				lat, _ := proc.Process(ctx, m) // publica out/DLQ (Writer Async=false)
				if lat > 0 {
					// log.Printf("[out] key=%q latency_ms=%.2f", string(m.Key), float64(lat.Milliseconds()))
				}
				ackCh <- m // só ack após processar/publicar
			}
		}(i)
	}

	// 5.2 committer (batch)
	doneCommit := make(chan struct{})
	go func() {
		defer close(doneCommit)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		batch := make([]kafkasdk.Message, 0, ackBatchSize)

		flush := func() {
			if len(batch) == 0 { return }
			_ = reader.CommitMessages(context.Background(), batch...)
			batch = batch[:0]
		}

		for {
			select {
			case m, ok := <-ackCh:
				if !ok { flush(); return }
				batch = append(batch, m)
				if len(batch) >= ackBatchSize {
					flush()
				}
			case <-t.C:
				flush()
			}
		}
	}()

	// 5.3 fetch loop
fetch:
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Printf("[shutdown] encerrando consumo")
				break fetch
			}
			log.Printf("[error] leitura Kafka: %v", err)
			continue
		}
		// log.Printf("[in] partition=%d offset=%d key=%q size=%d ts=%s",
		// 	m.Partition, m.Offset, string(m.Key), len(m.Value), m.Time.UTC().Format(time.RFC3339))
		msgCh <- m
	}

	// 6) shutdown ordenado
	close(msgCh)
	wg.Wait()
	close(ackCh)
	<-doneCommit
}
