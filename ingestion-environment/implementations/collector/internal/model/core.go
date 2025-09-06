package model

import "time"

type InboundEnvelope struct {
	DeviceID      string         `json:"deviceId"`
	DeviceType    string         `json:"deviceType"`
	EventType     string         `json:"eventType"`
	SchemaVersion string         `json:"schemaVersion"`
	Timestamp     *time.Time     `json:"timestamp"`
	Payload       map[string]any `json:"payload"`
}

type Metadata struct {
	EventID     string    `json:"eventId"`
	CollectedAt time.Time `json:"collectedAt"`
}

type OutboundEnvelope struct {
	InboundEnvelope
	Metadata Metadata `json:"metadata"`
}
