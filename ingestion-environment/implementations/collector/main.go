package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

// Required fields present in incoming JSON payloads
var requiredFields = []string{"deviceId", "deviceType", "eventType", "schemaVersion", "payload"}

// Config holds environment-driven configuration
type Config struct {
	MQTTBrokerURL  string
	MQTTClientID   string
	MQTTUsername   string
	MQTTPassword   string
	MQTTTopic      string
	MQTTQoS        byte

	KafkaBrokers           []string
	KafkaTopic             string
	KafkaDLQTopic          string
	KafkaTopicPartitions   int
	KafkaDLQPartitions     int
	KafkaReplicationFactor int

	Logger *log.Logger
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func loadConfig() (*Config, error) {
	brokers := getenv("KAFKA_BROKERS", "localhost:9092")
	// QoS helper (clamp 0..2)
	getenvQoS := func(key string, fallback byte) byte {
		val := os.Getenv(key)
		if val == "" { return fallback }
		if n, err := strconv.Atoi(val); err == nil {
			if n < 0 { n = 0 }
			if n > 2 { n = 2 }
			return byte(n)
		}
		return fallback
	}

	cfg := &Config{
		MQTTBrokerURL: getenv("MQTT_BROKER_URL", "tcp://vernemq:1883"),
		MQTTClientID:  getenv("MQTT_CLIENT_ID", "hems-collector"),
		MQTTUsername:  os.Getenv("MQTT_USERNAME"),
		MQTTPassword:  os.Getenv("MQTT_PASSWORD"),
		MQTTTopic:     getenv("MQTT_TOPIC", "ingestion/telemetry"),
		MQTTQoS:       getenvQoS("MQTT_QOS", 1), // << fix: byte + clamp 0..2

		KafkaBrokers:           strings.Split(brokers, ","),
		KafkaTopic:             getenv("KAFKA_TOPIC", "raw-events-topic"),
		KafkaDLQTopic:          getenv("KAFKA_DLQ_TOPIC", "raw-events-dlq"),
		KafkaTopicPartitions:   getenvInt("KAFKA_TOPIC_PARTITIONS", 3),
		KafkaDLQPartitions:     getenvInt("KAFKA_DLQ_PARTITIONS", 1),
		KafkaReplicationFactor: getenvInt("KAFKA_REPLICATION_FACTOR", 1),

		Logger: log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds),
	}
	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaBrokers[0] == "" {
		return nil, errors.New("KAFKA_BROKERS must not be empty")
	}
	return cfg, nil
}

// KafkaProducer wraps two writers (main and DLQ)
type KafkaProducer struct {
	main *kafka.Writer
	dlq  *kafka.Writer
}

func newKafkaProducer(cfg *Config) *KafkaProducer {
	balancer := &kafka.Hash{} // key-based partitioning
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

// ensureKafkaTopics creates topics if they don't exist yet
func ensureKafkaTopics(ctx context.Context, cfg *Config) error {
	bootstrap := cfg.KafkaBrokers[0]
	cfg.Logger.Printf("kafka: ensuring topics on bootstrap %s", bootstrap)

	// Dial any broker to discover the controller
	conn, err := kafka.DialContext(ctx, "tcp", bootstrap)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Helper to check existence
	exists := func(topic string) bool {
		parts, err := conn.ReadPartitions(topic)
		return err == nil && len(parts) > 0
	}

	// Dial the controller to create topics when necessary
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	ctrlConn, err := kafka.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	// Main topic
	if !exists(cfg.KafkaTopic) {
		cfg.Logger.Printf("kafka: creating topic %s (partitions=%d rf=%d)", cfg.KafkaTopic, cfg.KafkaTopicPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaTopic,
			NumPartitions:     cfg.KafkaTopicPartitions,
			ReplicationFactor: cfg.KafkaReplicationFactor,
		}); err != nil {
			return err
		}
	} else {
		cfg.Logger.Printf("kafka: topic %s already exists — skipping", cfg.KafkaTopic)
	}

	// DLQ topic
	if !exists(cfg.KafkaDLQTopic) {
		cfg.Logger.Printf("kafka: creating topic %s (partitions=%d rf=%d)", cfg.KafkaDLQTopic, cfg.KafkaDLQPartitions, cfg.KafkaReplicationFactor)
		if err := ctrlConn.CreateTopics(kafka.TopicConfig{
			Topic:             cfg.KafkaDLQTopic,
			NumPartitions:     cfg.KafkaDLQPartitions,
			ReplicationFactor: cfg.KafkaReplicationFactor,
		}); err != nil {
			return err
		}
	} else {
		cfg.Logger.Printf("kafka: topic %s already exists — skipping", cfg.KafkaDLQTopic)
	}

	return nil
}

// validatePayload ensures required fields are present and non-nil
func validatePayload(raw []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	for _, f := range requiredFields {
		v, ok := m[f]
		if !ok || v == nil {
			return nil, errors.New("missing field: " + f)
		}
		// treat empty string as missing for string-typed fields
		if s, isStr := v.(string); isStr && strings.TrimSpace(s) == "" {
			return nil, errors.New("empty field: " + f)
		}
	}
	return m, nil
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// OS signals for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigCh
		cfg.Logger.Printf("\nreceived signal: %v — shutting down...", s)
		cancel()
	}()

	// Ensure topics exist BEFORE creating the writers
	if err := ensureKafkaTopics(ctx, cfg); err != nil {
		cfg.Logger.Fatalf("kafka ensure topics error: %v", err)
	}

	producer := newKafkaProducer(cfg)
	defer producer.Close(ctx)

	// MQTT client options with auto-reconnect & retry
	handler := func(_ mqtt.Client, msg mqtt.Message) { handleMessage(ctx, cfg, producer, msg) }

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBrokerURL).
		SetClientID(cfg.MQTTClientID).
		SetOrderMatters(false). // higher concurrency in handlers
		SetCleanSession(false). // keep session for auto re-subscribe
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second)

	if cfg.MQTTUsername != "" {
		opts.SetUsername(cfg.MQTTUsername)
	}
	if cfg.MQTTPassword != "" {
		opts.SetPassword(cfg.MQTTPassword)
	}

	// On connect (or reconnect), (re)subscribe
	opts.OnConnect = func(c mqtt.Client) {
		cfg.Logger.Printf("connected to VerneMQ: %s", cfg.MQTTBrokerURL)
		if token := c.Subscribe(cfg.MQTTTopic, cfg.MQTTQoS, handler); token.Wait() && token.Error() != nil {
			cfg.Logger.Printf("mqtt subscribe error: %v", token.Error())
		} else {
			cfg.Logger.Printf("subscribed to topic: %s (QoS %d)", cfg.MQTTTopic, cfg.MQTTQoS)
		}
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) { cfg.Logger.Printf("mqtt connection lost: %v", err) }

	client := mqtt.NewClient(opts)

	// Connect loop with backoff (wait for broker readiness)
	backoff := 2 * time.Second
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			cfg.Logger.Printf("mqtt connect error: %v; retrying in %s", token.Error(), backoff)
			select {
			case <-time.After(backoff):
				if backoff < 30*time.Second {
					backoff *= 2
				}
			case <-ctx.Done():
				cfg.Logger.Println("context cancelled before mqtt connect")
				return
			}
			continue
		}
		break
	}

	// Block until context cancelled
	<-ctx.Done()
	cfg.Logger.Println("collector stopped")
}

func handleMessage(ctx context.Context, cfg *Config, prod *KafkaProducer, msg mqtt.Message) {
	receivedAt := time.Now().UTC()
	payload := msg.Payload()

	// Log message received from MQTT (with payload sample)
	cfg.Logger.Printf(
		"mqtt rx: topic=%s qos=%d mid=%d retained=%v bytes=%d payload=%s",
		msg.Topic(), msg.Qos(), msg.MessageID(), msg.Retained(), len(payload), truncate(payload, 512),
	)

	m, err := validatePayload(payload)
	if err != nil {
		cfg.Logger.Printf("invalid payload — sending to DLQ: %v | message: %s", err, truncate(payload, 512))
		// DLQ envelope
		dlq := map[string]any{
			"error":      err.Error(),
			"original":   json.RawMessage(payload),
			"topic":      msg.Topic(),
			"receivedAt": receivedAt.Format(time.RFC3339Nano),
		}
		buf, _ := json.Marshal(dlq)
		key := []byte("invalid")
		if err := prod.dlq.WriteMessages(ctx, kafka.Message{Key: key, Value: buf, Time: receivedAt}); err != nil {
			cfg.Logger.Printf("kafka write error (dlq): %v", err)
		} else {
			cfg.Logger.Printf("dlq OK: topic=%s bytes=%d", cfg.KafkaDLQTopic, len(buf))
		}
		return
	}

	// valid message → forward raw JSON to Kafka using deviceId as key (if string)
	var key []byte
	if v, ok := m["deviceId"].(string); ok {
		key = []byte(v)
	}
	if len(key) == 0 {
		key = []byte("unknown-device")
	}
	if err := prod.main.WriteMessages(ctx, kafka.Message{Key: key, Value: payload, Time: receivedAt}); err != nil {
		cfg.Logger.Printf("kafka write error (main): %v", err)
		return
	}
	cfg.Logger.Printf("kafka OK: topic=%s key=%s bytes=%d", cfg.KafkaTopic, string(key), len(payload))
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
