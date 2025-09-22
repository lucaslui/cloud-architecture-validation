// internal/broker/kafka.go
package broker

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/lucaslui/hems/batch-loader/internal/config"
	"github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	Reader *kafka.Reader
}

func NewKafkaClient(cfg *config.Config) *KafkaClient {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		GroupID: cfg.KafkaGroupID,
		Topic:   cfg.KafkaReaderTopic,

		// Tuning específico do reader:
		MinBytes:      cfg.KafkaReaderMinBytes,
		MaxBytes:      cfg.KafkaReaderMaxBytes,
		MaxWait:       time.Duration(cfg.KafkaReaderMaxWaitMs) * time.Millisecond,
		QueueCapacity: cfg.KafkaReaderQueueCapacity,

		ReadLagInterval: time.Duration(cfg.KafkaReaderReadLagIntervalMs) * time.Millisecond, // use -1 -> Duration(-1ms)

		// Estabilidade de grupo:
		SessionTimeout:    time.Duration(cfg.KafkaReaderSessionTimeoutMs) * time.Millisecond,
		HeartbeatInterval: time.Duration(cfg.KafkaReaderHeartbeatIntervalMs) * time.Millisecond,
		RebalanceTimeout:  time.Duration(cfg.KafkaReaderRebalanceTimeoutMs) * time.Millisecond,

		// Backoff de leitura:
		ReadBackoffMin: time.Duration(cfg.KafkaReaderReadBackoffMinMs) * time.Millisecond,
		ReadBackoffMax: time.Duration(cfg.KafkaReaderReadBackoffMaxMs) * time.Millisecond,

		// NÃO usar auto-commit; commit manual em batch (ackCh + CommitMessages)
		CommitInterval: 0,

		ErrorLogger: log.New(os.Stderr, "kafka-reader ERR ", 0),
	})
	return &KafkaClient{Reader: r}
}

// permita commits em lote
func (kc *KafkaClient) CommitMessages(ctx context.Context, msgs ...kafka.Message) error {
	return kc.Reader.CommitMessages(ctx, msgs...)
}

func (kc *KafkaClient) ConsumeMessages(ctx context.Context) (kafka.Message, error) {
	return kc.Reader.FetchMessage(ctx)
}

func (kc *KafkaClient) Close() error {
	return kc.Reader.Close()
}
