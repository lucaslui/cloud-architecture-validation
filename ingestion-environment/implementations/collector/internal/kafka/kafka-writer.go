package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
)

// KafkaProducer contém writers para tópico principal e DLQ
type KafkaProducer struct {
	main *kafka.Writer
	dlq  *kafka.Writer
}

func NewKafkaProducer(cfg *config.Config) *KafkaProducer {
	balancer := &kafka.Hash{}

	main := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaTopic,
		Balancer:     balancer,

		BatchSize:    1000,                 // alvo de msgs por batch
		BatchBytes:   1 << 20,              // ~1MB por batch
		BatchTimeout: 5 * time.Millisecond, // fecha lote rápido

		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Compression:  kafka.Snappy,
	}

	dlq := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaBrokers...),
		Topic:        cfg.KafkaDLQTopic,
		Balancer:     balancer,

		BatchSize:    200,
		BatchBytes:   512 << 10,            // 512KB
		BatchTimeout: 10 * time.Millisecond,

		RequiredAcks: kafka.RequireOne,
		Async:        true,
		Compression:  kafka.Snappy,
	}

	return &KafkaProducer{main: main, dlq: dlq}
}

func (p *KafkaProducer) Close(ctx context.Context) {
	_ = p.main.Close()
	_ = p.dlq.Close()
}

func (p *KafkaProducer) Send(ctx context.Context, key, value []byte, headers ...kafka.Header) error {
	return p.main.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}

func (p *KafkaProducer) SendDLQ(ctx context.Context, key, value []byte, headers ...kafka.Header) error {
	return p.dlq.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: headers,
	})
}


