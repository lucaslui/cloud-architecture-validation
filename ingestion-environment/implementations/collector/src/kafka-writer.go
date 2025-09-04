package main

import (
	"context"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer contém writers para tópico principal e DLQ
type KafkaProducer struct {
	main *kafka.Writer
	dlq  *kafka.Writer
}

func newKafkaProducer(cfg *Config) *KafkaProducer {
	balancer := &kafka.Hash{} // particionamento por chave
	return &KafkaProducer{
		main: &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        cfg.KafkaTopic,
			Balancer:     balancer,
			BatchSize:    100,
			RequiredAcks: kafka.RequireAll,
		},
		dlq: &kafka.Writer{
			Addr:         kafka.TCP(cfg.KafkaBrokers...),
			Topic:        cfg.KafkaDLQTopic,
			Balancer:     balancer,
			BatchSize:    10,
			RequiredAcks: kafka.RequireAll,
		},
	}
}

func (p *KafkaProducer) Close(ctx context.Context) {
	_ = p.main.Close()
	_ = p.dlq.Close()
}
