// internal/broker/kafka.go
package broker

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/lucaslui/hems/real-time-loader/internal/config"
)

type KafkaClient struct {
	Reader *kafka.Reader
}

func NewKafkaClient(cfg *config.Config) *KafkaClient {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        strings.Split(cfg.KafkaBrokers, ","),
		GroupID:        cfg.KafkaGroupID,
		Topic:          cfg.KafkaInputTopic,
		// StartOffset:  kafka.LastOffset, // REMOVIDO: usa committed offset
		MinBytes:       cfg.KafkaMinBytes,
		MaxBytes:       cfg.KafkaMaxBytes,
		MaxWait:        time.Duration(cfg.KafkaMaxWaitMs) * time.Millisecond,
		QueueCapacity:  2048,
		ReadLagInterval: -1,
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
