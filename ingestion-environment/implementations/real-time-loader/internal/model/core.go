package model

import "time"

type EnrichedEvent struct {
	DeviceID      string                 `json:"deviceId"`
	DeviceType    string                 `json:"deviceType"`
	EventType     string                 `json:"eventType"`
	SchemaVersion string                 `json:"schemaVersion"`
	Payload       map[string]interface{} `json:"payload"` // <- genérico
	Timestamp     time.Time              `json:"timestamp"`
	Metadata      struct {
		EventID     string    `json:"eventId"`
		ProcessedAt time.Time `json:"processedAt"`
		ReceivedAt  time.Time `json:"receivedAt"`
		SchemaRef   string    `json:"schemaRef"`
		Hash        string    `json:"hash"`
		Enrichment  struct {
			RegionID       string   `json:"regionId"`
			DeviceLocation string   `json:"deviceLocation"`
			DeviceCategory string   `json:"deviceCategory"`
			DeviceModel    string   `json:"deviceModel"`
			EndUserID      string   `json:"endUserId"`
			ContractType   string   `json:"contractType"`
			Programs       []string `json:"programs"`
		} `json:"enrichment"`
	} `json:"metadata"`
}
