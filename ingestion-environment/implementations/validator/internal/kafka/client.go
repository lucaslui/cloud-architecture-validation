// internal/kafka/client.go
package kafka

import (
	"time"

	"github.com/lucaslui/hems/validator/internal/config"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaMinBytes = 10_000     // 10KB
	kafkaMaxBytes = 10_000_000 // 10MB
)

type KafkaClient struct {
	Reader *kafka.Reader
	Writer *kafka.Writer
}

func NewKafkaClient(cfg config.Config) *KafkaClient {
	return &KafkaClient{
		Reader: NewReader(cfg),
		Writer: NewWriter(cfg.Brokers, cfg.OutputTopic),
	}
}

func NewReader(cfg config.Config) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:         cfg.Brokers,
		GroupID:         cfg.GroupID,
		Topic:           cfg.InputTopic,
		MinBytes:        kafkaMinBytes,
		MaxBytes:        kafkaMaxBytes,
		MaxWait:         50 * time.Millisecond, // formar lotes
		QueueCapacity:   2048,                  // mais msgs em buffer
		ReadLagInterval: -1,                    // menos overhead
		// Importante: não force StartOffset aqui; deixe o committed offset do grupo prevalecer.
	})
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    1000,                 // alvo de 1000 msgs
		BatchBytes:   1 << 20,              // ~1MB
		BatchTimeout: 5 * time.Millisecond, // baixa latência com batching
		Compression:  kafka.Snappy,         // ou LZ4/ZSTD
		RequiredAcks: kafka.RequireOne,     // throughput > durabilidade (ajuste conforme SLO)
		Async:        true,                 // libera o produtor
	}
}
