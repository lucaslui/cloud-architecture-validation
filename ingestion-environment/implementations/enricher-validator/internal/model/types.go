package model

import "time"
import "encoding/json"

const (
	AllowedEventTypeTelemetry = "Telemetry"
	AllowedEventTypeState     = "State"
	EnvelopeContentTypeJSON   = "application/json"
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

type Meta struct {
	EventID       string     `json:"eventId"`
	ProcessedAt   time.Time  `json:"processedAt"`
	ReceivedAt    *time.Time `json:"receivedAt,omitempty"`
	SourceTopic   string     `json:"sourceTopic"`
	SchemaRef     string     `json:"schemaRef"`
	ContentType   string     `json:"contentType"`
	Hash          string     `json:"hash"`
	CorrelationID string     `json:"correlationId,omitempty"`
	Enrichment    *Enrichment `json:"enrichment,omitempty"`
}

type EnrichedEvent struct {
	Envelope
	Meta Meta `json:"meta"`
}

type DLQEvent struct {
	Envelope
	Error string `json:"error"`
	Stage string `json:"stage"`
	Meta  Meta   `json:"meta"`
}

func IsAllowedEventType(t string) bool {
	return t == AllowedEventTypeTelemetry || t == AllowedEventTypeState
}
