package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Logger *log.Logger

	// Kafka
	KafkaBrokers   string // "kafka:9092"
	KafkaGroupID   string // "batch-loader"
	KafkaTopic     string // "enriched-events-topic"
	KafkaMinBytes  int    // 1e4
	KafkaMaxBytes  int    // 1e6
	KafkaMaxWaitMs int    // 200

	// Batching
	BatchMaxRecords  int           // p.ex. 100_000
	BatchMaxInterval time.Duration // p.ex. 2m
	BatchMaxBytes    int64         // opcional: máx. ~128MB por arquivo

	// MinIO (S3 compatível)
	S3Endpoint  string // "minio:9000"
	S3AccessKey string
	S3SecretKey string
	S3UseTLS    bool
	S3Bucket    string // "hems-datalake"
	S3BasePath  string // "bronze/enriched"

	// Parquet
	ParquetCompression string // "SNAPPY" (recomendado) ou "ZSTD"
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func getbool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if v == "1" || v == "true" || v == "TRUE" {
			return true
		}
		return false
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func getint64(key string, def int64) int64 {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return def
}

func LoadConfig() *Config {
	return &Config{
		Logger:             log.New(os.Stdout, "", log.LstdFlags),
		KafkaBrokers:       getenv("KAFKA_BROKERS", "kafka:9092"),
		KafkaGroupID:       getenv("KAFKA_GROUP_ID", "batch-loader"),
		KafkaTopic:         getenv("KAFKA_INPUT_TOPIC", "enriched-events-topic"),
		KafkaMinBytes:      getint("KAFKA_MIN_BYTES", 10_000),
		KafkaMaxBytes:      getint("KAFKA_MAX_BYTES", 1_000_000),
		KafkaMaxWaitMs:     getint("KAFKA_MAX_WAIT_MS", 200),
		BatchMaxRecords:    getint("BATCH_MAX_RECORDS", 100_000),
		BatchMaxInterval:   getdur("BATCH_MAX_INTERVAL", 2*time.Minute),
		BatchMaxBytes:      getint64("BATCH_MAX_BYTES", 128*1024*1024), // 128MB
		S3Endpoint:         getenv("S3_ENDPOINT", "minio:9000"),
		S3AccessKey:        getenv("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey:        getenv("S3_SECRET_KEY", "minioadmin"),
		S3UseTLS:           getbool("S3_USE_TLS", false),
		S3Bucket:           getenv("S3_BUCKET", "hems-datalake"),
		S3BasePath:         getenv("S3_BASE_PATH", "bronze/hems"),
		ParquetCompression: getenv("PARQUET_COMPRESSION", "SNAPPY"),
	}
}
