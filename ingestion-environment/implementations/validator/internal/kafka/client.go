package kafka

import (
	"github.com/lucaslui/hems/validator/internal/config"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaMinBytes = 1_000
	kafkaMaxBytes = 10_000_000
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
		Brokers:     cfg.Brokers,
		GroupID:     cfg.GroupID,
		Topic:       cfg.InputTopic,
		MinBytes:    kafkaMinBytes,
		MaxBytes:    kafkaMaxBytes,
		StartOffset: kafka.FirstOffset,
	})
}

func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
}
