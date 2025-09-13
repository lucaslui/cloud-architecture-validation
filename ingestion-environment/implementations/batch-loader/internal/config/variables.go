package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	KafkaBrokers           []string
	KafkaGroupID           string
	KafkaReaderTopic       string
	KafkaTopicPartitions   int
	KafkaRetentionMs       int64
	KafkaCompression       string
	KafkaReplicationFactor int
	KafkaAckBatchSize      int

	KafkaReaderMinBytes            int
	KafkaReaderMaxBytes            int
	KafkaReaderMaxWaitMs           int
	KafkaReaderQueueCapacity       int
	KafkaReaderReadLagIntervalMs   int
	KafkaReaderSessionTimeoutMs    int
	KafkaReaderHeartbeatIntervalMs int
	KafkaReaderRebalanceTimeoutMs  int
	KafkaReaderReadBackoffMinMs    int
	KafkaReaderReadBackoffMaxMs    int

	ProcessingWorkers int

	BatchMaxRecords     int
	BatchMaxBytes       int64
	BatchMaxIntervalSec int

	S3Endpoint  string
	S3AccessKey string
	S3SecretKey string
	S3UseTLS    bool
	S3Bucket    string
	S3BasePath  string

	ParquetCompression string // "SNAPPY" (recomendado) ou "ZSTD"
}

func (c *Config) String() string {
	return fmt.Sprintf(`
Kafka:
  Brokers:            %v
  GroupID:            %s
  InputTopic:         %s
  TopicPartitions:    %d
  RetentionMs:        %d
  Compression:        %s
  ReplicationFactor:  %d
  AckBatchSize:       %d

Kafka Reader:
  MinBytes:            %d
  MaxBytes:            %d
  MaxWaitMs:           %d
  QueueCapacity:       %d
  ReadLagIntervalMs:   %d
  SessionTimeoutMs:    %d
  HeartbeatIntervalMs: %d
  RebalanceTimeoutMs:  %d
  ReadBackoffMinMs:    %d
  ReadBackoffMaxMs:    %d

Workers:
  Processing:         %d

Batcher:
  MaxRecords:         %d
  MaxBytes:           %d
  MaxIntervalSec:     %d

S3:
  Endpoint:           %s
  AccessKey:          %s
  SecretKey:          %s
  UseTLS:             %t
  Bucket:             %s
  BasePath:           %s

Parquet:
  Compression:        %s
`,
		c.KafkaBrokers,
		c.KafkaGroupID,
		c.KafkaReaderTopic,
		c.KafkaTopicPartitions,
		c.KafkaRetentionMs,
		c.KafkaCompression,
		c.KafkaReplicationFactor,
		c.KafkaAckBatchSize,

		c.KafkaReaderMinBytes,
		c.KafkaReaderMaxBytes,
		c.KafkaReaderMaxWaitMs,
		c.KafkaReaderQueueCapacity,
		c.KafkaReaderReadLagIntervalMs,
		c.KafkaReaderSessionTimeoutMs,
		c.KafkaReaderHeartbeatIntervalMs,
		c.KafkaReaderRebalanceTimeoutMs,
		c.KafkaReaderReadBackoffMinMs,
		c.KafkaReaderReadBackoffMaxMs,

		c.ProcessingWorkers,

		c.BatchMaxRecords,
		c.BatchMaxBytes,
		c.BatchMaxIntervalSec,

		c.S3Endpoint,
		c.S3AccessKey,
		strings.Repeat("*", len(c.S3SecretKey)),
		c.S3UseTLS,
		c.S3Bucket,
		c.S3BasePath,

		c.ParquetCompression,
	)
}

type errList []string

func (e *errList) addf(format string, a ...any) { *e = append(*e, fmt.Sprintf(format, a...)) }
func (e *errList) add(msg string)               { *e = append(*e, msg) }
func (e *errList) has() bool                    { return len(*e) > 0 }

func getRequired(key string, errs *errList) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		errs.addf("faltando %s", key)
	}
	return v
}

func getRequiredInt(key string, errs *errList) int {
	v := getRequired(key, errs)
	if v == "" {
		return 0
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		errs.addf("%s inválido (esperado int): %q", key, v)
		return 0
	}
	return n
}

func getRequiredInt64(key string, errs *errList) int64 {
	v := getRequired(key, errs)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		errs.addf("%s inválido (esperado int64): %q", key, v)
		return 0
	}
	return n
}

func getRequiredBool(key string, errs *errList) bool {
	v := strings.ToLower(getRequired(key, errs))
	switch v {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		if v != "" { // só acusa formato inválido se a env existia
			errs.addf("%s inválido (use true/false ou 1/0): %q", key, v)
		}
		return false
	}
}

func ensureOneOf(key, val string, allowed []string, errs *errList) {
	ok := false
	for _, a := range allowed {
		if val == a {
			ok = true
			break
		}
	}
	if !ok {
		errs.addf("%s inválido (permitidos: %s): %q", key, strings.Join(allowed, ", "), val)
	}
}

func parseBrokers(list string, errs *errList) []string {
	var out []string
	if list == "" {
		return out
	}
	for _, b := range strings.Split(list, ",") {
		if s := strings.TrimSpace(b); s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		errs.add("KAFKA_BROKERS inválido (lista vazia)")
	}
	return out
}

func LoadConfig(logger *log.Logger) (*Config, error) {
	var errs errList

	kafkaBrokers := parseBrokers(getRequired("KAFKA_BROKERS", &errs), &errs)
	kafkaGroupID := getRequired("KAFKA_GROUP_ID", &errs)
	kafkaReaderTopic := getRequired("KAFKA_READER_TOPIC", &errs)
	kafkaTopicPartitions := getRequiredInt("KAFKA_TOPIC_PARTITIONS", &errs)
	kafkaCompression := getRequired("KAFKA_COMPRESSION", &errs)
	kafkaRetentionMs := getRequiredInt64("KAFKA_RETENTION_MS", &errs)
	kafkaReplicationFactor := getRequiredInt("KAFKA_REPLICATION_FACTOR", &errs)
	kafkaAckBatchSize := getRequiredInt("KAFKA_ACK_BATCH_SIZE", &errs)

	kafkaReaderMinBytes := getRequiredInt("KAFKA_READER_MIN_BYTES", &errs)
	kafkaReaderMaxBytes := getRequiredInt("KAFKA_READER_MAX_BYTES", &errs)
	kafkaReaderMaxWaitMs := getRequiredInt("KAFKA_READER_MAX_WAIT_MS", &errs)
	kafkaReaderQueueCapacity := getRequiredInt("KAFKA_READER_QUEUE_CAPACITY", &errs)
	kafkaReaderReadLagIntervalMs := getRequiredInt("KAFKA_READER_READ_LAG_INTERVAL_MS", &errs)
	kafkaReaderSessionTimeoutMs := getRequiredInt("KAFKA_READER_SESSION_TIMEOUT_MS", &errs)
	kafkaReaderHeartbeatIntervalMs := getRequiredInt("KAFKA_READER_HEARTBEAT_INTERVAL_MS", &errs)
	kafkaReaderRebalanceTimeoutMs := getRequiredInt("KAFKA_READER_REBALANCE_TIMEOUT_MS", &errs)
	kafkaReaderReadBackoffMinMs := getRequiredInt("KAFKA_READER_READ_BACKOFF_MIN_MS", &errs)
	kafkaReaderReadBackoffMaxMs := getRequiredInt("KAFKA_READER_READ_BACKOFF_MAX_MS", &errs)

	processingWorkers := getRequiredInt("PROCESSING_WORKERS", &errs)

	BatchMaxRecords := getRequiredInt("BATCH_MAX_RECORDS", &errs)
	BatchMaxBytes := getRequiredInt64("BATCH_MAX_BYTES", &errs)
	BatchMaxIntervalSec := getRequiredInt("BATCH_MAX_INTERVAL_SEC", &errs)

	S3Endpoint := getRequired("S3_ENDPOINT", &errs)
	S3AccessKey := getRequired("S3_ACCESS_KEY", &errs)
	S3SecretKey := getRequired("S3_SECRET_KEY", &errs)
	S3UseTLS := getRequiredBool("S3_USE_TLS", &errs)
	S3Bucket := getRequired("S3_BUCKET", &errs)
	S3BasePath := getRequired("S3_BASE_PATH", &errs)

	ParquetCompression := getRequired("PARQUET_COMPRESSION", &errs)

	ensureOneOf("KAFKA_COMPRESSION", kafkaCompression, []string{"none", "gzip", "snappy", "lz4", "zstd"}, &errs)

	if len(kafkaBrokers) == 0 {
		errs.add("KAFKA_BROKERS deve ter ao menos 1 broker")
	}
	if kafkaGroupID == "" {
		errs.add("KAFKA_GROUP_ID não pode ser vazio")
	}
	if kafkaReaderTopic == "" {
		errs.add("KAFKA_READER_TOPIC não pode ser vazio")
	}
	if kafkaTopicPartitions <= 0 {
		errs.add("KAFKA_TOPIC_PARTITIONS deve ser > 0")
	}
	if kafkaCompression == "" {
		errs.add("KAFKA_COMPRESSION não pode ser vazio")
	}
	if kafkaReplicationFactor <= 0 {
		errs.add("KAFKA_REPLICATION_FACTOR deve ser > 0")
	}
	if kafkaReplicationFactor > len(kafkaBrokers) {
		errs.add("KAFKA_REPLICATION_FACTOR não pode ser maior que o número de brokers em KAFKA_BROKERS")
	}
	if kafkaAckBatchSize <= 0 {
		errs.add("KAFKA_ACK_BATCH_SIZE deve ser > 0")
	}
	if kafkaRetentionMs < -1 {
		errs.add("KAFKA_RETENTION_MS deve ser >= -1")
	}
	if kafkaReaderMinBytes < 0 {
		errs.add("KAFKA_READER_MIN_BYTES deve ser >= 0")
	}
	if kafkaReaderMaxBytes <= 0 {
		errs.add("KAFKA_READER_MAX_BYTES deve ser > 0")
	}
	if kafkaReaderMaxBytes < kafkaReaderMinBytes {
		errs.add("KAFKA_READER_MAX_BYTES deve ser >= KAFKA_READER_MIN_BYTES")
	}
	if kafkaReaderMaxWaitMs <= 0 {
		errs.add("KAFKA_READER_MAX_WAIT_MS deve ser > 0")
	}
	if kafkaReaderQueueCapacity <= 0 {
		errs.add("KAFKA_READER_QUEUE_CAPACITY deve ser > 0")
	}
	if kafkaReaderReadLagIntervalMs < -1 {
		errs.add("KAFKA_READER_READ_LAG_INTERVAL_MS deve ser -1 ou mais")
	}
	if kafkaReaderSessionTimeoutMs <= 0 {
		errs.add("KAFKA_READER_SESSION_TIMEOUT_MS deve ser > 0")
	}
	if kafkaReaderHeartbeatIntervalMs <= 0 {
		errs.add("KAFKA_READER_HEARTBEAT_INTERVAL_MS deve ser > 0")
	}
	if kafkaReaderRebalanceTimeoutMs <= 0 {
		errs.add("KAFKA_READER_REBALANCE_TIMEOUT_MS deve ser > 0")
	}
	if kafkaReaderReadBackoffMinMs < 0 {
		errs.add("KAFKA_READER_READ_BACKOFF_MIN_MS deve ser >= 0")
	}
	if kafkaReaderReadBackoffMaxMs <= 0 {
		errs.add("KAFKA_READER_READ_BACKOFF_MAX_MS deve ser > 0")
	}
	if kafkaReaderReadBackoffMaxMs < kafkaReaderReadBackoffMinMs {
		errs.add("KAFKA_READER_READ_BACKOFF_MAX_MS deve ser >= KAFKA_READER_READ_BACKOFF_MIN_MS")
	}
	if processingWorkers <= 0 {
		errs.add("PROCESSING_WORKERS deve ser > 0")
	}
	if BatchMaxRecords <= 0 {
		errs.add("BATCH_MAX_RECORDS deve ser > 0")
	}
	if BatchMaxBytes <= 0 {
		errs.add("BATCH_MAX_BYTES deve ser > 0")
	}
	if BatchMaxIntervalSec <= 0 {
		errs.add("BATCH_MAX_INTERVAL_SEC deve ser > 0")
	}
	if S3Endpoint == "" {
		errs.add("S3_ENDPOINT não pode ser vazio")
	}
	if S3AccessKey == "" {
		errs.add("S3_ACCESS_KEY não pode ser vazio")
	}
	if S3SecretKey == "" {
		errs.add("S3_SECRET_KEY não pode ser vazio")
	}
	if S3Bucket == "" {
		errs.add("S3_BUCKET não pode ser vazio")
	}
	if ParquetCompression == "" {
		errs.add("PARQUET_COMPRESSION não pode ser vazio")
	} else {
		ensureOneOf("PARQUET_COMPRESSION", ParquetCompression, []string{"SNAPPY", "ZSTD"}, &errs)
	}

	if errs.has() {
		for _, e := range errs {
			logger.Printf("[config] %s", e)
		}
		return nil, errors.New("variáveis de ambiente faltando/invalidas — ver logs acima")
	}

	return &Config{
		KafkaBrokers:           kafkaBrokers,
		KafkaGroupID:           kafkaGroupID,
		KafkaReaderTopic:       kafkaReaderTopic,
		KafkaTopicPartitions:   kafkaTopicPartitions,
		KafkaRetentionMs:       kafkaRetentionMs,
		KafkaCompression:       kafkaCompression,
		KafkaReplicationFactor: kafkaReplicationFactor,
		KafkaAckBatchSize:      kafkaAckBatchSize,

		KafkaReaderMinBytes:            kafkaReaderMinBytes,
		KafkaReaderMaxBytes:            kafkaReaderMaxBytes,
		KafkaReaderMaxWaitMs:           kafkaReaderMaxWaitMs,
		KafkaReaderQueueCapacity:       kafkaReaderQueueCapacity,
		KafkaReaderReadLagIntervalMs:   kafkaReaderReadLagIntervalMs,
		KafkaReaderSessionTimeoutMs:    kafkaReaderSessionTimeoutMs,
		KafkaReaderHeartbeatIntervalMs: kafkaReaderHeartbeatIntervalMs,
		KafkaReaderRebalanceTimeoutMs:  kafkaReaderRebalanceTimeoutMs,
		KafkaReaderReadBackoffMinMs:    kafkaReaderReadBackoffMinMs,
		KafkaReaderReadBackoffMaxMs:    kafkaReaderReadBackoffMaxMs,

		ProcessingWorkers: processingWorkers,

		BatchMaxRecords:     BatchMaxRecords,
		BatchMaxBytes:       BatchMaxBytes,
		BatchMaxIntervalSec: BatchMaxIntervalSec,

		S3Endpoint:  S3Endpoint,
		S3AccessKey: S3AccessKey,
		S3SecretKey: S3SecretKey,
		S3UseTLS:    S3UseTLS,
		S3Bucket:    S3Bucket,
		S3BasePath:  S3BasePath,

		ParquetCompression: ParquetCompression,
	}, nil
}
