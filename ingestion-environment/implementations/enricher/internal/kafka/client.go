// internal/kafka/client.go
package kafka

import (
	"time"

	"github.com/lucaslui/hems/enricher-validator/internal/config"
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
		Brokers:        cfg.Brokers,
		GroupID:        cfg.GroupID,
		Topic:          cfg.InputTopic,
		MinBytes:       kafkaMinBytes,
		MaxBytes:       kafkaMaxBytes,
		MaxWait:        50 * time.Millisecond, // incentiva fetches maiores
		QueueCapacity:  2048,                  // mais msgs em buffer
		ReadLagInterval: -1,                   // menor overhead
		// REMOVIDO: StartOffset (use offset committed do grupo)
	})
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},           // mantém ordem por key
		BatchSize:    1000,                    // alvo de 1000 msgs por lote
		BatchBytes:   1 << 20,                 // ~1MB
		BatchTimeout: 5 * time.Millisecond,    // fecha lote rápido
		Compression:  kafka.Snappy,            // ou LZ4/ZSTD
		RequiredAcks: kafka.RequireOne,        // throughput ok (use RequireAll se quiser +durabilidade)
		Async:        false,                   // **IMPORTANTE**: neste hop Kafka→Kafka preserve at-least-once
	}
}
