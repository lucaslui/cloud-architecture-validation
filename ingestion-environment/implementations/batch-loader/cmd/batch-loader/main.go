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

type workResult struct {
	msg kafka.Message
	rec data.Record
	err error
}

func main() {
	logger := config.GetLogger()

	cfg, err := config.LoadConfig(logger)
	if err != nil {
		logger.Fatalf("[boot] configuração inválida: %v", err)
	}
	logger.Printf("[info] batch loader configs loaded:%s", cfg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Garante/cria tópico de entrada com partitions/RF/compression/retention
	if err := broker.EnsureKafkaTopics(ctx, cfg, logger); err != nil {
		logger.Fatalf("[error] kafka ensure topics error: %v", err)
	}

	kafkaClient := broker.NewKafkaClient(cfg)
	defer kafkaClient.Close()

	minio, err := storage.NewMinIO(cfg.S3Endpoint, cfg.S3AccessKey, cfg.S3SecretKey, cfg.S3UseTLS, cfg.S3Bucket)
	if err != nil {
		logger.Fatalf("s3 client error: %v", err)
	}
	if err := minio.EnsureBucket(ctx); err != nil {
		logger.Fatalf("ensure bucket error: %v", err)
	}

	// Batcher central (apenas o aggregator escreve nele)
	batcher := data.NewBatcher(
		cfg.BatchMaxRecords,
		cfg.BatchMaxBytes,
		cfg.BatchMaxIntervalSec,
		minio,
		cfg.S3BasePath,
		cfg.ParquetCompression,
		cfg.ParquetWriterParallel,
	)

	// Canais com folga para absorver bursts
	msgCh := make(chan kafka.Message, cfg.MsgChannelBuffer) // fetch → workers
	resCh := make(chan workResult, cfg.ResultChannelBuffer) // workers → aggregator

	// 1) Fetcher: drena Kafka continuamente
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
				time.Sleep(time.Duration(cfg.KafkaFetchRetryMs) * time.Millisecond)
				continue
			}
			msgCh <- m
		}
	}()

	// 2) Pool de workers: parse JSON → Record
	var workersWG sync.WaitGroup
	workersWG.Add(cfg.ProcessingWorkers)
	for i := 0; i < cfg.ProcessingWorkers; i++ {
		go func(id int) {
			defer workersWG.Done()
			for m := range msgCh {
				var evt model.InboundBatchLoaderEnvelope
				if err := json.Unmarshal(m.Value, &evt); err != nil {
					resCh <- workResult{msg: m, err: err}
					continue
				}
				resCh <- workResult{msg: m, rec: data.ToRecord(evt)}
			}
		}(i)
	}

	// 3) Aggregator: único que mexe no batcher e no commit
	ticker := time.NewTicker(time.Duration(cfg.FlushTickMs) * time.Millisecond)
	defer ticker.Stop()

	// guarda o último msg por partição desde o último flush
	lastByPartition := make(map[int]kafka.Message)

	commitBatch := func() {
		if len(lastByPartition) == 0 {
			return
		}
		acks := make([]kafka.Message, 0, len(lastByPartition))
		for _, m := range lastByPartition {
			acks = append(acks, m)
		}

		for i := 0; i < len(acks); i += cfg.KafkaAckBatchSize {
			end := i + cfg.KafkaAckBatchSize
			if end > len(acks) {
				end = len(acks)
			}
			cctx, ccancel := context.WithTimeout(context.Background(), time.Duration(cfg.KafkaCommitTimeoutMs)*time.Millisecond)
			if err := kafkaClient.Reader.CommitMessages(cctx, acks[i:end]...); err != nil {
				logger.Printf("commit error: %v", err)
				ccancel()
				return
			}
			ccancel()
		}
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
			// segurança: commit somente após persistência
			commitBatch()
		}
	}

	// Loop do aggregator
	var aggWG sync.WaitGroup
	aggWG.Add(1)
	go func() {
		defer aggWG.Done()
		for {
			select {
			case <-ctx.Done():
				logger.Println("signal received, flushing and stopping...")
				flushNow("shutdown")
				return

			case <-ticker.C:
				if batcher.ShouldFlushByInterval() {
					flushNow("by interval")
				}

			case r, ok := <-resCh:
				if !ok {
					return
				}
				if r.err != nil {
					logger.Printf("json decode error partition=%d offset=%d: %v", r.msg.Partition, r.msg.Offset, r.err)
					continue
				}

				if batcher.Add(r.rec) { // atingiu limite por quantidade/bytes
					flushNow("by size/bytes")
				}

				// marca último offset por partição
				lastByPartition[r.msg.Partition] = r.msg

				// garante flush por tempo mesmo sob carga contínua
				if batcher.ShouldFlushByInterval() {
					flushNow("by interval")
				}
			}
		}
	}()

	<-ctx.Done()
	close(msgCh)
	workersWG.Wait()
	close(resCh)
	aggWG.Wait()
	flushNow("finalize")
	fetchWG.Wait()
}
