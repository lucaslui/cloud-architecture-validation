package validate

import (
	"encoding/json"
	"errors"
	"strings"
)

var requiredFields = []string{"deviceId", "deviceType", "eventType", "schemaVersion", "payload"}

func ValidatePayload(raw []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	for _, f := range requiredFields {
		v, ok := m[f]
		if !ok || v == nil {
			return nil, errors.New("missing field: " + f)
		}
		if s, isStr := v.(string); isStr && strings.TrimSpace(s) == "" {
			return nil, errors.New("empty field: " + f)
		}
	}
	return m, nil
}

func Truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}
