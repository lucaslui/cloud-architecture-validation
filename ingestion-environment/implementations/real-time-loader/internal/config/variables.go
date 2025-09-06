package config

import "os"

type Config struct {
	KafkaBrokers    string
	KafkaGroupID    string
	KafkaInputTopic string

	InfluxURL    string
	InfluxToken  string
	InfluxOrg    string
	InfluxBucket string

	InfluxMeasurementTemplate string
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func LoadEnvVariables() *Config {
	return &Config{
		KafkaBrokers:    os.Getenv("KAFKA_BROKERS"),
		KafkaGroupID:    os.Getenv("KAFKA_GROUP_ID"),
		KafkaInputTopic: os.Getenv("KAFKA_INPUT_TOPIC"),

		InfluxURL:    os.Getenv("INFLUX_URL"),
		InfluxToken:  os.Getenv("INFLUX_TOKEN"),
		InfluxOrg:    os.Getenv("INFLUX_ORG"),
		InfluxBucket: os.Getenv("INFLUX_BUCKET"),

		InfluxMeasurementTemplate: getenv("INFLUX_MEASUREMENT_TEMPLATE", "telemetry_%s"),
	}
}
