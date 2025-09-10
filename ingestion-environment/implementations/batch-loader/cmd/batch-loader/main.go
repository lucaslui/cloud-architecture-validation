package main

import (
	"context"
	"encoding/json"
	"errors"
	"os/signal"
	"syscall"
	"time"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/batch-loader/internal/broker"
	"github.com/lucaslui/hems/batch-loader/internal/config"
	"github.com/lucaslui/hems/batch-loader/internal/data"
	"github.com/lucaslui/hems/batch-loader/internal/model"
	"github.com/lucaslui/hems/batch-loader/internal/storage"
)

func main() {
	cfg := config.LoadConfig()

	cfg.Logger.Printf("[boot] batch-loader | brokers=%s group=%s topic=%s bucket=%s base=%s compression=%s",
		cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaTopic, cfg.S3Bucket, cfg.S3BasePath, cfg.ParquetCompression)

	cfg.Logger.Printf("[batch] limits: max_records=%d max_bytes=%d max_interval=%s",
		cfg.BatchMaxRecords, cfg.BatchMaxBytes, cfg.BatchMaxInterval)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	minio, err := storage.NewMinIO(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3UseTLS, cfg.S3Bucket)
	if err != nil { cfg.Logger.Fatalf("s3 client error: %v", err) }
	if err := minio.EnsureBucket(ctx); err != nil { cfg.Logger.Fatalf("ensure bucket error: %v", err) }

	reader := broker.NewKafkaReader(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaTopic,
		cfg.KafkaMinBytes, cfg.KafkaMaxBytes, cfg.KafkaMaxWaitMs)
	defer reader.Close()

	// Pré-aloca o buffer do batcher
	batcher := data.NewBatcher(cfg.BatchMaxRecords, cfg.BatchMaxBytes, cfg.BatchMaxInterval, minio, cfg.S3BasePath, cfg.ParquetCompression)

	// Canal de ingestão dos fetches
	msgCh := make(chan kafka.Message, 5000)

	// 1) Fetch goroutine: lê o mais rápido possível
	var fetchWG sync.WaitGroup
	fetchWG.Add(1)
	go func() {
		defer fetchWG.Done()
		for {
			m, err := reader.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				cfg.Logger.Printf("kafka fetch error: %v", err)
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
		if len(lastByPartition) == 0 { return }
		acks := make([]kafka.Message, 0, len(lastByPartition))
		for _, m := range lastByPartition {
			acks = append(acks, m)
		}
		if err := reader.CommitMessages(context.Background(), acks...); err != nil {
			cfg.Logger.Printf("commit error: %v", err)
		}
		// Limpa janelas de commit para próxima rodada
		lastByPartition = make(map[int]kafka.Message)
	}

flushNow := func(reason string) {
		n, err := batcher.Flush(ctx)
		if err != nil {
			cfg.Logger.Printf("flush error (%s): %v", reason, err)
			return
		}
		if n > 0 {
			cfg.Logger.Printf("[flush] %s wrote %d records", reason, n)
			// Só após persistir no S3 fazemos commit dos offsets incluídos
			commitBatch()
		}
	}

loop:
	for {
		select {
		case <-ctx.Done():
			cfg.Logger.Println("signal received, flushing and stopping...")
			flushNow("shutdown")
			break loop

		case <-ticker.C:
			if batcher.ShouldFlushByInterval() {
				flushNow("by interval")
			}

		case m := <-msgCh:
			var event model.InboundEnvelope
			if err := json.Unmarshal(m.Value, &event); err != nil {
				cfg.Logger.Printf("json decode error partition=%d offset=%d: %v", m.Partition, m.Offset, err)
				// Mensagem inválida: escolha a política (p.ex. pular/estacionar). Aqui apenas ignora.
				continue
			}

			record := data.ToRecord(event)
			cfg.Logger.Printf("record received partition=%d offset=%d device_id=%s type=%s ts=%s",
				m.Partition, m.Offset, record.DeviceID, record.EventType,
				time.UnixMilli(record.Timestamp).UTC().Format(time.RFC3339))

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
	cfg.Logger.Println("batch-loader stopped")
}
