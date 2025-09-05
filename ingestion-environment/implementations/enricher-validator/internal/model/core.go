package model

import (
	"encoding/json"
	"time"
)

type Envelope struct {
	DeviceID      string          `json:"deviceId"`
	DeviceType    string          `json:"deviceType"`
	EventType     string          `json:"eventType"`
	SchemaVersion string          `json:"schemaVersion"`
	Payload       json.RawMessage `json:"payload"`
	Timestamp     *time.Time      `json:"timestamp,omitempty"`
	CorrelationID string          `json:"correlationId,omitempty"`
}

type Enrichment struct {
	RegionID       string   `json:"regionId"`
	DeviceLocation string   `json:"deviceLocation"`
	DeviceCategory string   `json:"deviceCategory"`
	DeviceModel    string   `json:"deviceModel"`
	EndUserID      string   `json:"endUserId"`
	ContractType   string   `json:"contractType"`
	Programs       []string `json:"programs"`
}

type Metadata struct {
	EventID       string      `json:"eventId"`
	ProcessedAt   time.Time   `json:"processedAt"`
	ReceivedAt    *time.Time  `json:"receivedAt,omitempty"`
	SchemaRef     string      `json:"schemaRef"`
	Hash          string      `json:"hash"`
	CorrelationID string      `json:"correlationId,omitempty"`
	Enrichment    *Enrichment `json:"enrichment,omitempty"`
}

type EnrichedEvent struct {
	Envelope
	Metadata Metadata `json:"metadata"`
}

type DLQEvent struct {
	Envelope
	Error    string   `json:"error"`
	Stage    string   `json:"stage"`
	Metadata Metadata `json:"metadata"`
}
