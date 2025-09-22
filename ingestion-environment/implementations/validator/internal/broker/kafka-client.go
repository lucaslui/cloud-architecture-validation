package broker

import (
	"log"
	"os"
	"strings"
	"time"

	"github.com/lucaslui/hems/validator/internal/config"
	"github.com/segmentio/kafka-go"
)

type KafkaClient struct {
	MainProducer *kafka.Writer
	DLQProducer  *kafka.Writer
	Consumer     *kafka.Reader
}

func NewKafkaClient(cfg *config.Config) *KafkaClient {
	return &KafkaClient{
		MainProducer: NewKafkaProducer(cfg, cfg.KafkaWriterTopic),
		DLQProducer:  NewKafkaProducer(cfg, cfg.KafkaDLQTopic),
		Consumer:     NewKafkaConsumer(cfg),
	}
}

func NewKafkaProducer(cfg *config.Config, topic string) *kafka.Writer {
	balancer := &kafka.Hash{}

	return &kafka.Writer{
		Addr:     kafka.TCP(cfg.KafkaBrokers...),
		Topic:    topic,
		Balancer: balancer,

		BatchSize:    cfg.KafkaWriterBatchSize,
		BatchBytes:   cfg.KafkaWriterBatchBytes,
		BatchTimeout: time.Duration(cfg.KafkaWriterBatchTimeoutMs) * time.Millisecond,

		RequiredAcks: parseAcks(cfg.KafkaWriterRequiredAcks),
		MaxAttempts:  cfg.KafkaWriterMaxAttempts,
		Async:        true,
		Compression:  parseCompression(cfg.KafkaCompression),
	}
}

func NewKafkaConsumer(cfg *config.Config) *kafka.Reader {
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaBrokers,
		GroupID: cfg.KafkaGroupID,
		Topic:   cfg.KafkaReaderTopic,
		Dialer:  dialer,

		// Tuning específico do reader:
		MinBytes:      cfg.KafkaReaderMinBytes,
		MaxBytes:      cfg.KafkaReaderMaxBytes,
		MaxWait:       time.Duration(cfg.KafkaReaderMaxWaitMs) * time.Millisecond,
		QueueCapacity: cfg.KafkaReaderQueueCapacity,

		ReadLagInterval:       time.Duration(cfg.KafkaReaderReadLagIntervalMs) * time.Millisecond, // use -1 -> Duration(-1ms)
		WatchPartitionChanges: false,
		// Estabilidade de grupo:
		SessionTimeout:    time.Duration(cfg.KafkaReaderSessionTimeoutMs) * time.Millisecond,
		HeartbeatInterval: time.Duration(cfg.KafkaReaderHeartbeatIntervalMs) * time.Millisecond,
		RebalanceTimeout:  time.Duration(cfg.KafkaReaderRebalanceTimeoutMs) * time.Millisecond,

		// Backoff de leitura:
		ReadBackoffMin: time.Duration(cfg.KafkaReaderReadBackoffMinMs) * time.Millisecond,
		ReadBackoffMax: time.Duration(cfg.KafkaReaderReadBackoffMaxMs) * time.Millisecond,

		// NÃO usar auto-commit; commit manual em batch (ackCh + CommitMessages)
		CommitInterval: 0,

		Logger:      log.New(os.Stdout, "kafka-reader ", 0),
		ErrorLogger: log.New(os.Stderr, "kafka-reader ERR ", 0),
	})
}

func parseAcks(s string) kafka.RequiredAcks {
	switch strings.ToLower(s) {
	case "none":
		return kafka.RequireNone
	case "all":
		return kafka.RequireAll
	default:
		return kafka.RequireOne
	}
}

func parseCompression(s string) kafka.Compression {
	switch strings.ToLower(s) {
	case "", "none", "no", "off", "0":
		return kafka.Compression(0)
	case "gzip":
		return kafka.Gzip
	case "snappy":
		return kafka.Snappy
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	default:
		return kafka.Snappy
	}
}

func (p *KafkaClient) Close() {
	_ = p.MainProducer.Close()
	_ = p.DLQProducer.Close()
	_ = p.Consumer.Close()
}
