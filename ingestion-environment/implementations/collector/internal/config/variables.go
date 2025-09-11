package config

import (
	"errors"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Config struct {
	// MQTT
	MQTTBrokerURL    string
	MQTTClientID     string
	MQTTUsername     string // opcional
	MQTTPassword     string // opcional
	MQTTTopic        string
	MQTTQoS          byte
	MQTTChannelDepth uint

	// Kafka
	KafkaBrokers           []string
	KafkaTopic             string
	KafkaDLQTopic          string
	KafkaTopicPartitions   int
	KafkaDLQPartitions     int
	KafkaReplicationFactor int
	KafkaBatchSize         int
	KafkaBatchBytes        int64
	KafkaBatchTimeoutMs    int
	KafkaCompression       string
	KafkaRequiredAcks      string
	KafkaMaxAttempts       int
	KafkaRetentionMs       int64

	// Concorrência / Dispatcher
	ProcessingWorkers     int
	WorkerQueueSize       int
	DispatcherCapacity    int
	DispatcherMaxBatch    int
	DispatcherTickMs      int
	WorkQueueStrategy     string
	WorkQueueEnqTimeoutMs int
}

func (c *Config) String() string {
	return fmt.Sprintf(`
MQTT:
  BrokerURL:     %s
  ClientID:      %s
  Username:      %s
  Topic:         %s
  QoS:           %d
  ChannelDepth:  %d

Kafka:
  Brokers:           %v
  Topic:             %s
  DLQTopic:          %s
  Partitions:        %d
  DLQPartitions:     %d
  ReplicationFactor: %d
  BatchSize:         %d
  BatchBytes:        %d
  BatchTimeoutMs:    %d
  Compression:       %s
  RequiredAcks:      %s
  MaxAttempts:       %d
  RetentionMs:       %d

Workers:
  Processing:        %d
  QueueSize:         %d

Dispatcher:
  Capacity:          %d
  MaxBatch:          %d
  TickMs:            %d
  Strategy:          %s
  EnqTimeoutMs:      %d

`, c.MQTTBrokerURL, c.MQTTClientID, c.MQTTUsername, c.MQTTTopic, c.MQTTQoS, c.MQTTChannelDepth,
		c.KafkaBrokers, c.KafkaTopic, c.KafkaDLQTopic, c.KafkaTopicPartitions, c.KafkaDLQPartitions, c.KafkaReplicationFactor,
		c.KafkaBatchSize, c.KafkaBatchBytes, c.KafkaBatchTimeoutMs, c.KafkaCompression, c.KafkaRequiredAcks, c.KafkaMaxAttempts,
		c.KafkaRetentionMs, c.ProcessingWorkers, c.WorkerQueueSize,
		c.DispatcherCapacity, c.DispatcherMaxBatch, c.DispatcherTickMs, c.WorkQueueStrategy, c.WorkQueueEnqTimeoutMs)
}

type errList []string

func (e *errList) addf(format string, a ...any) {
	*e = append(*e, fmt.Sprintf(format, a...))
}
func (e *errList) add(msg string) { *e = append(*e, msg) }
func (e *errList) has() bool      { return len(*e) > 0 }

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
func getRequiredUInt(key string, errs *errList) uint {
	v := getRequired(key, errs)
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		errs.addf("%s inválido (esperado uint): %q", key, v)
		return 0
	}
	return uint(n)
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
func getRequiredQoS(key string, errs *errList) byte {
	n := getRequiredInt(key, errs)
	if n < 0 || n > 2 {
		errs.addf("%s inválido (0..2): %d", key, n)
		if n < 0 {
			n = 0
		}
		if n > 2 {
			n = 2
		}
	}
	return byte(n)
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

type mqttCfg struct {
	brokerURL    string
	clientID     string
	username     string
	password     string
	topic        string
	qos          byte
	channelDepth uint
}

func loadMQTT(errs *errList) mqttCfg {
	return mqttCfg{
		brokerURL:    getRequired("MQTT_BROKER_URL", errs),
		clientID:     getRequired("MQTT_CLIENT_ID", errs),
		username:     os.Getenv("MQTT_USERNAME"), // opcional
		password:     os.Getenv("MQTT_PASSWORD"), // opcional
		topic:        getRequired("MQTT_TOPIC", errs),
		qos:          getRequiredQoS("MQTT_QOS", errs),
		channelDepth: getRequiredUInt("MQTT_CHANNEL_DEPTH", errs),
	}
}

type kafkaCfg struct {
	brokers           []string
	topic             string
	dlqTopic          string
	partitions        int
	dlqPartitions     int
	replicationFactor int
	batchSize         int
	batchBytes        int64
	batchTimeoutMs    int
	compression       string
	requiredAcks      string
	maxAttempts       int
	retentionMs       int64
}

func loadKafka(errs *errList) kafkaCfg {
	brokers := parseBrokers(getRequired("KAFKA_BROKERS", errs), errs)
	comp := getRequired("KAFKA_COMPRESSION", errs)
	acks := getRequired("KAFKA_REQUIRED_ACKS", errs)
	ensureOneOf("KAFKA_COMPRESSION", comp, []string{"none", "gzip", "snappy", "lz4", "zstd"}, errs)
	ensureOneOf("KAFKA_REQUIRED_ACKS", acks, []string{"none", "one", "all"}, errs)

	return kafkaCfg{
		brokers:           brokers,
		topic:             getRequired("KAFKA_TOPIC", errs),
		dlqTopic:          getRequired("KAFKA_DLQ_TOPIC", errs),
		partitions:        getRequiredInt("KAFKA_TOPIC_PARTITIONS", errs),
		dlqPartitions:     getRequiredInt("KAFKA_DLQ_PARTITIONS", errs),
		replicationFactor: getRequiredInt("KAFKA_REPLICATION_FACTOR", errs),
		batchSize:         getRequiredInt("KAFKA_BATCH_SIZE", errs),
		batchBytes:        getRequiredInt64("KAFKA_BATCH_BYTES", errs),
		batchTimeoutMs:    getRequiredInt("KAFKA_BATCH_TIMEOUT_MS", errs),
		compression:       comp,
		requiredAcks:      acks,
		maxAttempts:       getRequiredInt("KAFKA_MAX_ATTEMPTS", errs),
		retentionMs:       getRequiredInt64("KAFKA_RETENTION_MS", errs),
	}
}

type concCfg struct {
	processingWorkers     int
	workerQueueSize       int
	dispatcherCapacity    int
	dispatcherMaxBatch    int
	dispatcherTickMs      int
	workQueueStrategy     string
	workQueueEnqTimeoutMs int
}

func loadConcurrency(errs *errList) concCfg {
	ws := getRequired("WORK_QUEUE_STRATEGY", errs)
	ensureOneOf("WORK_QUEUE_STRATEGY", ws, []string{"drop", "block"}, errs)

	return concCfg{
		processingWorkers:     getRequiredInt("PROCESSING_WORKERS", errs),
		workerQueueSize:       getRequiredInt("WORKER_QUEUE_SIZE", errs),
		dispatcherCapacity:    getRequiredInt("DISPATCHER_CAPACITY", errs),
		dispatcherMaxBatch:    getRequiredInt("DISPATCHER_MAX_BATCH", errs),
		dispatcherTickMs:      getRequiredInt("DISPATCHER_TICK_MS", errs),
		workQueueStrategy:     ws,
		workQueueEnqTimeoutMs: getRequiredInt("WORK_QUEUE_ENQ_TIMEOUT_MS", errs),
	}
}

func validateSanity(m mqttCfg, k kafkaCfg, c concCfg, errs *errList) {
	if c.processingWorkers <= 0 {
		errs.addf("PROCESSING_WORKERS deve ser > 0 (sugerido: %d)", runtime.NumCPU()*2)
	}
	if c.workerQueueSize <= 0 {
		errs.add("WORKER_QUEUE_SIZE deve ser > 0")
	}
	if c.dispatcherCapacity <= 0 {
		errs.add("DISPATCHER_CAPACITY deve ser > 0")
	}
	if c.dispatcherMaxBatch <= 0 {
		errs.add("DISPATCHER_MAX_BATCH deve ser > 0")
	}
	if c.dispatcherTickMs <= 0 {
		errs.add("DISPATCHER_TICK_MS deve ser > 0")
	}
	if m.channelDepth == 0 {
		errs.add("MQTT_CHANNEL_DEPTH deve ser > 0")
	}
	if len(k.brokers) == 0 {
		errs.add("KAFKA_BROKERS inválido (lista vazia)")
	}
	if k.partitions <= 0 {
		errs.add("KAFKA_TOPIC_PARTITIONS deve ser > 0")
	}
	if k.dlqPartitions <= 0 {
		errs.add("KAFKA_DLQ_PARTITIONS deve ser > 0")
	}
	if k.replicationFactor <= 0 {
		errs.add("KAFKA_REPLICATION_FACTOR deve ser > 0")
	}
	if k.replicationFactor > len(k.brokers) {
		errs.add("KAFKA_REPLICATION_FACTOR não pode ser maior que o número de brokers em KAFKA_BROKERS")
	}
	if k.batchSize <= 0 {
		errs.add("KAFKA_BATCH_SIZE deve ser > 0")
	}
	if k.batchBytes <= 0 {
		errs.add("KAFKA_BATCH_BYTES deve ser > 0")
	}
	if k.batchTimeoutMs <= 0 {
		errs.add("KAFKA_BATCH_TIMEOUT_MS deve ser > 0")
	}
	if k.maxAttempts <= 0 {
		errs.add("KAFKA_MAX_ATTEMPTS deve ser > 0")
	}
}

func LoadConfig() (*Config, error) {
	var errs errList

	m := loadMQTT(&errs)
	k := loadKafka(&errs)
	c := loadConcurrency(&errs)

	validateSanity(m, k, c, &errs)

	if errs.has() {
		for _, e := range errs {
			log.Printf("[config] %s", e)
		}
		return nil, errors.New("variáveis de ambiente faltando/invalidas — ver logs acima")
	}

	cfg := &Config{
		// MQTT
		MQTTBrokerURL:    m.brokerURL,
		MQTTClientID:     m.clientID,
		MQTTUsername:     m.username,
		MQTTPassword:     m.password,
		MQTTTopic:        m.topic,
		MQTTQoS:          m.qos,
		MQTTChannelDepth: m.channelDepth,

		// Kafka
		KafkaBrokers:           k.brokers,
		KafkaTopic:             k.topic,
		KafkaDLQTopic:          k.dlqTopic,
		KafkaTopicPartitions:   k.partitions,
		KafkaDLQPartitions:     k.dlqPartitions,
		KafkaReplicationFactor: k.replicationFactor,
		KafkaBatchSize:         k.batchSize,
		KafkaBatchBytes:        k.batchBytes,
		KafkaBatchTimeoutMs:    k.batchTimeoutMs,
		KafkaCompression:       k.compression,
		KafkaRequiredAcks:      k.requiredAcks,
		KafkaMaxAttempts:       k.maxAttempts,
		KafkaRetentionMs:       k.retentionMs,

		// Concorrência / Dispatcher
		ProcessingWorkers:     c.processingWorkers,
		WorkerQueueSize:       c.workerQueueSize,
		DispatcherCapacity:    c.dispatcherCapacity,
		DispatcherMaxBatch:    c.dispatcherMaxBatch,
		DispatcherTickMs:      c.dispatcherTickMs,
		WorkQueueStrategy:     c.workQueueStrategy,
		WorkQueueEnqTimeoutMs: c.workQueueEnqTimeoutMs,
	}

	return cfg, nil
}
