package main

import (
	"context"
	"encoding/json"
	"errors"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/batch-loader/internal/broker"
	"github.com/lucaslui/hems/batch-loader/internal/config"
	"github.com/lucaslui/hems/batch-loader/internal/data"
	"github.com/lucaslui/hems/batch-loader/internal/model"
	"github.com/lucaslui/hems/batch-loader/internal/storage"
)

func main() {
	logger := config.GetLogger()

	cfg, err := config.LoadConfig(logger)
	if err != nil {
		logger.Fatalf("[boot] configuração inválida: %v", err)
	}

	logger.Printf("[info] batch loader configs loaded:%s", cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	minio, err := storage.NewMinIO(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3UseTLS, cfg.S3Bucket)
	if err != nil {
		logger.Fatalf("s3 client error: %v", err)
	}
	if err := minio.EnsureBucket(ctx); err != nil {
		logger.Fatalf("ensure bucket error: %v", err)
	}

	kafkaClient := broker.NewKafkaClient(cfg)

	// Pré-aloca o buffer do batcher
	batcher := data.NewBatcher(cfg.BatchMaxRecords, cfg.BatchMaxBytes, cfg.BatchMaxIntervalSec, minio, cfg.S3BasePath, cfg.ParquetCompression)

	// Canal de ingestão dos fetches
	msgCh := make(chan kafka.Message, 5000)

	// 1) Fetch goroutine: lê o mais rápido possível
	var fetchWG sync.WaitGroup
	fetchWG.Add(1)
	go func() {
		defer fetchWG.Done()
		for {
			m, err := kafkaClient.Reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				logger.Printf("kafka fetch error: %v", err)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			msgCh <- m
		}
	}()

	// 2) Agregador + flush + commit (loop principal)
	ticker := time.NewTicker(time.Second) // decide flush periódico
	defer ticker.Stop()

	// Guardamos o "último" message por partição desde o último flush
	lastByPartition := make(map[int]kafka.Message)

	commitBatch := func() {
		if len(lastByPartition) == 0 {
			return
		}
		acks := make([]kafka.Message, 0, len(lastByPartition))
		for _, m := range lastByPartition {
			acks = append(acks, m)
		}
		if err := kafkaClient.Reader.CommitMessages(context.Background(), acks...); err != nil {
			logger.Printf("commit error: %v", err)
		}
		// Limpa janelas de commit para próxima rodada
		lastByPartition = make(map[int]kafka.Message)
	}

	flushNow := func(reason string) {
		n, err := batcher.Flush(ctx)
		if err != nil {
			logger.Printf("flush error (%s): %v", reason, err)
			return
		}
		if n > 0 {
			logger.Printf("[flush] %s wrote %d records", reason, n)
			// Só após persistir no S3 fazemos commit dos offsets incluídos
			commitBatch()
		}
	}

loop:
	for {
		select {
		case <-ctx.Done():
			logger.Println("signal received, flushing and stopping...")
			flushNow("shutdown")
			break loop

		case <-ticker.C:
			if batcher.ShouldFlushByInterval() {
				flushNow("by interval")
			}

		case m := <-msgCh:
			var event model.InboundBatchLoaderEnvelope
			if err := json.Unmarshal(m.Value, &event); err != nil {
				logger.Printf("json decode error partition=%d offset=%d: %v", m.Partition, m.Offset, err)
				// Mensagem inválida: escolha a política (p.ex. pular/estacionar). Aqui apenas ignora.
				continue
			}

			record := data.ToRecord(event)
			// logger.Printf("record received partition=%d offset=%d device_id=%s type=%s ts=%s",
			// 	m.Partition, m.Offset, record.DeviceID, record.EventType,
			// 	time.UnixMilli(record.Timestamp).UTC().Format(time.RFC3339))

			// Agrega para flush posterior
			if batcher.Add(record) {
				// Se atingiu limite por quantidade/bytes → flush
				flushNow("by size/bytes")
			}

			// Marca último offset por partição desde o último flush
			lastByPartition[m.Partition] = m

			// Mesmo com tráfego contínuo, respeite flush por tempo
			if batcher.ShouldFlushByInterval() {
				flushNow("by interval")
			}
		}
	}

	// Shutdown ordenado
	close(msgCh)
	fetchWG.Wait()
	logger.Println("batch-loader stopped")
}
