package model

import (
	"encoding/json"
	"time"
)

type InboundEnvelope struct {
	DeviceID        string           `json:"deviceId"`
	DeviceType      string           `json:"deviceType"`
	EventType       string           `json:"eventType"`
	SchemaVersion   string           `json:"schemaVersion"`
	Timestamp       *time.Time       `json:"timestamp"`
	Payload         json.RawMessage  `json:"payload"`
	InboundMetadata *InboundMetadata `json:"metadata"`
}

type InboundMetadata struct {
	EventID     string    `json:"eventId"`
	CollectedAt time.Time `json:"collectedAt"`
}

type OutboundEnvelope struct {
	InboundEnvelope
	Metadata OutboundMetadata `json:"metadata"`
}

type OutboundMetadata struct {
	EventID     string    `json:"eventId"`
	CollectedAt time.Time `json:"collectedAt"`
	ValidatedAt time.Time `json:"validatedAt"`
}

type OutboundDLQEnvelope struct {
	InboundEnvelope
	Error    string           `json:"error"`
	Stage    string           `json:"stage"`
	Metadata OutboundMetadata `json:"metadata"`
}
