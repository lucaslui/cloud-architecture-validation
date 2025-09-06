package broker

import (
	"context"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	Reader *kafka.Reader
}

func NewKafkaClient(brokers string, groupID string, topic string) *KafkaClient {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         strings.Split(brokers, ","),
		GroupID:         groupID,
		Topic:           topic,
		StartOffset:     kafka.LastOffset,
		CommitInterval:  time.Second,
		MinBytes:        1,
		MaxBytes:        10e6,
		ReadLagInterval: -1,
	})
	return &KafkaClient{Reader: r}
}

func (kc *KafkaClient) ConsumeMessages(ctx context.Context) (kafka.Message, error) {
	return kc.Reader.FetchMessage(ctx)
}

func (kc *KafkaClient) CommitMessages(ctx context.Context, msg kafka.Message) error {
	return kc.Reader.CommitMessages(ctx, msg)
}

func (kc *KafkaClient) Close() error {
	return kc.Reader.Close()
}
