package kafka

import (
	"context"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/collector/internal/config"
)

// KafkaProducer contém writers para tópico principal e DLQ
type KafkaProducer struct {
	main *kafka.Writer
	dlq  *kafka.Writer
}

func NewKafkaProducer(cfg *config.Config) *KafkaProducer {
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
