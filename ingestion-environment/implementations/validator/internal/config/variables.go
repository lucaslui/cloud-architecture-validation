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
	KafkaWriterTopic       string
	KafkaDLQTopic          string
	KafkaTopicPartitions   int
	KafkaDLQPartitions     int
	KafkaRetentionMs       int64
	KafkaCompression       string
	KafkaReplicationFactor int
	KafkaAckBatchSize      int

	KafkaWriterBatchSize      int
	KafkaWriterBatchBytes     int64
	KafkaWriterBatchTimeoutMs int
	KafkaWriterRequiredAcks   string
	KafkaWriterMaxAttempts    int

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

	RedisAddr           string
	RedisPassword       string // opcional
	RedisDB             int
	RedisNamespace      string
	RedisUsePubSub      bool
	RedisInvalidateChan string
}

func (c *Config) String() string {
	return fmt.Sprintf(`
Kafka:
  Brokers:            %v
  GroupID:            %s
  InputTopic:         %s
  OutputTopic:        %s
  DLQTopic:           %s
  TopicPartitions:    %d
  DLQPartitions:      %d
  RetentionMs:        %d
  Compression:        %s
  ReplicationFactor:  %d
  AckBatchSize:       %d

Kafka Writer:
  BatchSize:          %d
  BatchBytes:         %d
  BatchTimeoutMs:     %d
  RequiredAcks:       %s
  MaxAttempts:        %d

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

Redis:
  Addr:               %s
  DB:                 %d
  Namespace:          %s
  UsePubSub:          %t
  InvalidateChannel:  %s
`,
		c.KafkaBrokers,
		c.KafkaGroupID,
		c.KafkaReaderTopic,
		c.KafkaWriterTopic,
		c.KafkaDLQTopic,
		c.KafkaTopicPartitions,
		c.KafkaDLQPartitions,
		c.KafkaRetentionMs,
		c.KafkaCompression,
		c.KafkaReplicationFactor,
		c.KafkaAckBatchSize,

		c.KafkaWriterBatchSize,
		c.KafkaWriterBatchBytes,
		c.KafkaWriterBatchTimeoutMs,
		c.KafkaWriterRequiredAcks,
		c.KafkaWriterMaxAttempts,

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

		c.RedisAddr,
		c.RedisDB,
		c.RedisNamespace,
		c.RedisUsePubSub,
		c.RedisInvalidateChan,
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

func LoadConfig() (*Config, error) {
	var errs errList

	kafkaBrokers := parseBrokers(getRequired("KAFKA_BROKERS", &errs), &errs)
	kafkaGroupID := getRequired("KAFKA_GROUP_ID", &errs)
	kafkaReaderTopic := getRequired("KAFKA_READER_TOPIC", &errs)
	kafkaWriterTopic := getRequired("KAFKA_WRITER_TOPIC", &errs)
	kafkaDLQTopic := getRequired("KAFKA_DLQ_TOPIC", &errs)
	kafkaTopicPartitions := getRequiredInt("KAFKA_TOPIC_PARTITIONS", &errs)
	kafkaDLQPartitions := getRequiredInt("KAFKA_DLQ_PARTITIONS", &errs)
	kafkaCompression := getRequired("KAFKA_COMPRESSION", &errs)
	kafkaRetentionMs := getRequiredInt64("KAFKA_RETENTION_MS", &errs)
	kafkaReplicationFactor := getRequiredInt("KAFKA_REPLICATION_FACTOR", &errs)
	kafkaAckBatchSize := getRequiredInt("KAFKA_ACK_BATCH_SIZE", &errs)

	kafkaWriterBatchSize := getRequiredInt("KAFKA_WRITER_BATCH_SIZE", &errs)
	kafkaWriterBatchBytes := getRequiredInt("KAFKA_WRITER_BATCH_BYTES", &errs)
	kafkaWriterBatchTimeoutMs := getRequiredInt("KAFKA_WRITER_BATCH_TIMEOUT_MS", &errs)
	kafkaWriterRequiredAcks := getRequired("KAFKA_WRITER_REQUIRED_ACKS", &errs)
	kafkaWriterMaxAttempts := getRequiredInt("KAFKA_WRITER_MAX_ATTEMPTS", &errs)

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

	redisAddr := getRequired("REDIS_ADDR", &errs)
	redisPassword := os.Getenv("REDIS_PASSWORD") // opcional
	redisNamespace := getRequired("REDIS_NAMESPACE", &errs)
	redisUsePubSub := getRequiredBool("REDIS_USE_PUBSUB", &errs)
	redisInvalidateChan := getRequired("REDIS_INVALIDATE_CHANNEL", &errs)
	redisDB := getRequiredInt("REDIS_DB", &errs)

	ensureOneOf("KAFKA_COMPRESSION", kafkaCompression, []string{"none", "gzip", "snappy", "lz4", "zstd"}, &errs)
	ensureOneOf("KAFKA_WRITER_REQUIRED_ACKS", kafkaWriterRequiredAcks, []string{"none", "one", "all"}, &errs)

	if len(kafkaBrokers) == 0 {
		errs.add("KAFKA_BROKERS deve ter ao menos 1 broker")
	}
	if kafkaGroupID == "" {
		errs.add("KAFKA_GROUP_ID não pode ser vazio")
	}
	if kafkaReaderTopic == "" {
		errs.add("KAFKA_READER_TOPIC não pode ser vazio")
	}
	if kafkaWriterTopic == "" {
		errs.add("KAFKA_WRITER_TOPIC não pode ser vazio")
	}
	if kafkaDLQTopic == "" {
		errs.add("KAFKA_DLQ_TOPIC não pode ser vazio")
	}
	if kafkaTopicPartitions <= 0 {
		errs.add("KAFKA_TOPIC_PARTITIONS deve ser > 0")
	}
	if kafkaDLQPartitions <= 0 {
		errs.add("KAFKA_DLQ_TOPIC_PARTITIONS deve ser > 0")
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
		errs.add("KAFKA_RETENTION_MS deve ser > -1")
	}
	if kafkaWriterBatchSize <= 0 {
		errs.add("KAFKA_WRITER_BATCH_SIZE deve ser > 0")
	}
	if kafkaWriterBatchBytes <= 0 {
		errs.add("KAFKA_WRITER_BATCH_BYTES deve ser > 0")
	}
	if kafkaWriterBatchTimeoutMs <= 0 {
		errs.add("KAFKA_WRITER_BATCH_TIMEOUT_MS deve ser > 0")
	}
	if kafkaWriterMaxAttempts <= 0 {
		errs.add("KAFKA_WRITER_MAX_ATTEMPTS deve ser > 0")
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
	if redisDB < 0 {
		errs.add("REDIS_DB deve ser >= 0")
	}
	if redisAddr == "" {
		errs.add("REDIS_ADDR não pode ser vazio")
	}
	if redisNamespace == "" {
		errs.add("REDIS_NAMESPACE não pode ser vazio")
	}
	if redisUsePubSub && redisInvalidateChan == "" {
		errs.add("REDIS_INVALIDATE_CHANNEL não pode ser vazio se REDIS_USE_PUBSUB for true")
	}

	if errs.has() {
		for _, e := range errs {
			log.Printf("[config] %s", e)
		}
		return nil, errors.New("variáveis de ambiente faltando/invalidas — ver logs acima")
	}

	return &Config{
		KafkaBrokers:           kafkaBrokers,
		KafkaGroupID:           kafkaGroupID,
		KafkaReaderTopic:       kafkaReaderTopic,
		KafkaWriterTopic:       kafkaWriterTopic,
		KafkaDLQTopic:          kafkaDLQTopic,
		KafkaTopicPartitions:   kafkaTopicPartitions,
		KafkaDLQPartitions:     kafkaDLQPartitions,
		KafkaRetentionMs:       kafkaRetentionMs,
		KafkaCompression:       kafkaCompression,
		KafkaReplicationFactor: kafkaReplicationFactor,
		KafkaAckBatchSize:      kafkaAckBatchSize,

		KafkaWriterBatchSize:      kafkaWriterBatchSize,
		KafkaWriterBatchBytes:     int64(kafkaWriterBatchBytes),
		KafkaWriterBatchTimeoutMs: kafkaWriterBatchTimeoutMs,
		KafkaWriterRequiredAcks:   kafkaWriterRequiredAcks,
		KafkaWriterMaxAttempts:    kafkaWriterMaxAttempts,

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

		RedisAddr:           redisAddr,
		RedisPassword:       redisPassword,
		RedisDB:             redisDB,
		RedisNamespace:      redisNamespace,
		RedisUsePubSub:      redisUsePubSub,
		RedisInvalidateChan: redisInvalidateChan,
	}, nil
}
