package model

import (
	"encoding/json"
	"time"
)

type InboundBatchLoaderEnvelope struct {
	DeviceID      string          `json:"deviceId"`
	DeviceType    string          `json:"deviceType"`
	EventType     string          `json:"eventType"`
	SchemaVersion string          `json:"schemaVersion"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload"`
	Context       struct {
		RegionID       string   `json:"regionId"`
		DeviceLocation string   `json:"deviceLocation"`
		DeviceCategory string   `json:"deviceCategory"`
		DeviceModel    string   `json:"deviceModel"`
		EndUserID      string   `json:"endUserId"`
		ContractType   string   `json:"contractType"`
		Programs       []string `json:"programs"`
	} `json:"context"`
	Metadata struct {
		EventID     string     `json:"eventId"`
		CollectedAt *time.Time `json:"collectedAt"`
		ValidatedAt *time.Time `json:"validatedAt"`
		EnrichedAt  *time.Time `json:"enrichedAt"`
	} `json:"metadata"`
}

// Estrutura "bronze semiestruturada" — payload_json guarda leituras dinâmicas
type OutboundBatchLoaderRecord struct {
	DeviceID      string `parquet:"name=device_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceType    string `parquet:"name=device_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	EventType     string `parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8"`
	SchemaVersion string `parquet:"name=schema_version, type=BYTE_ARRAY, convertedtype=UTF8"`
	Timestamp     int64  `parquet:"name=timestamp, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Payload       string `parquet:"name=payload, type=BYTE_ARRAY, convertedtype=UTF8"`

	RegionId       string `parquet:"name=region_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceLocation string `parquet:"name=device_location, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceCategory string `parquet:"name=device_category, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	DeviceModel    string `parquet:"name=device_model, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	EndUserID      string `parquet:"name=end_user_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	ContractType   string `parquet:"name=contract_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Programs       string `parquet:"name=programs, type=BYTE_ARRAY, convertedtype=UTF8"`

	EventID string `parquet:"name=event_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

func ToMillis(t time.Time) int64 { return t.UTC().UnixNano() / 1e6 }
