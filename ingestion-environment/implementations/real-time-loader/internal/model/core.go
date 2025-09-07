package model

import "time"

type InboundEnvelope struct {
	DeviceID      string                 `json:"deviceId"`
	DeviceType    string                 `json:"deviceType"`
	EventType     string                 `json:"eventType"`
	SchemaVersion string                 `json:"schemaVersion"`
	Timestamp     time.Time             `json:"timestamp"`
	Payload       map[string]interface{} `json:"payload"`
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
