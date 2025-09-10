package database

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"

	"github.com/lucaslui/hems/real-time-loader/internal/config"
	"github.com/lucaslui/hems/real-time-loader/internal/model"
)

type InfluxDB struct {
	Client   influxdb2.Client
	WriteAPI api.WriteAPI
	errsDone chan struct{}
}

func NewInfluxDB(cfg *config.Config) *InfluxDB {
	opts := influxdb2.DefaultOptions().
		SetBatchSize(uint(cfg.InfluxBatchSize)).
		SetFlushInterval(uint(cfg.InfluxFlushIntervalMs))
		// (opcional) .SetRetryInterval(1000).SetMaxRetries(5) …

	client := influxdb2.NewClientWithOptions(cfg.InfluxURL, cfg.InfluxToken, opts)
	w := client.WriteAPI(cfg.InfluxOrg, cfg.InfluxBucket)

	// drena o canal de erros do writer em background
	errsDone := make(chan struct{})
	go func() {
		defer close(errsDone)
		for err := range w.Errors() {
			// logue, exporte métrica, etc.
			fmt.Printf("[influx-async] write error: %v\n", err)
		}
	}()

	return &InfluxDB{Client: client, WriteAPI: w, errsDone: errsDone}
}

func (db *InfluxDB) Close() {
	if db == nil || db.Client == nil {
		return
	}
	// força flush do buffer pendente antes de fechar
	db.WriteAPI.Flush()
	db.Client.Close()
	// aguarda dreno do canal de erros
	<-db.errsDone
}

// agora o WriteEvent apenas ENFILEIRA o ponto (não bloqueia)
func (db *InfluxDB) WriteEvent(_ context.Context, evt *model.InboundEnvelope) error {
	point := db.buildPoint(evt)
	db.WriteAPI.WritePoint(point)
	return nil
}

func (db *InfluxDB) buildPoint(evt *model.InboundEnvelope) *write.Point {
	tags := map[string]string{
		"deviceId":       evt.DeviceID,
		"deviceType":     evt.DeviceType,
		"deviceModel":    evt.Context.DeviceModel,
		"deviceCategory": evt.Context.DeviceCategory,
		"deviceLocation": evt.Context.DeviceLocation,
		"schemaVersion":  evt.SchemaVersion,
		"regionId":       evt.Context.RegionID,
	}

	flat := make(map[string]interface{})
	flatten("", evt.Payload, flat)

	fields := make(map[string]interface{})
	for k, v := range flat {
		if fv, ok := normalizeFieldValue(v); ok {
			fields[sanitizeFieldKey(k)] = fv
		}
	}

	return write.NewPoint(evt.EventType, tags, fields, evt.Timestamp)
}

// flatten: obj/array -> chaves separadas por "_"
func flatten(prefix string, v interface{}, out map[string]interface{}) {
	key := func(k string) string {
		if prefix == "" {
			return k
		}
		return prefix + "_" + k
	}
	switch t := v.(type) {
	case map[string]interface{}:
		for k, val := range t {
			flatten(key(k), val, out)
		}
	case []interface{}:
		parts := make([]string, 0, len(t))
		for _, item := range t {
			parts = append(parts, toScalarString(item))
		}
		out[prefix] = strings.Join(parts, ",")
	default:
		out[prefix] = t
	}
}

func toScalarString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case bool:
		if x {
			return "true"
		}
		return "false"
	case float64, float32, int, int64, uint64, int32, uint32, int16, uint16, int8, uint8:
		return fmt.Sprintf("%v", x)
	case map[string]interface{}:
		tmp := make(map[string]interface{})
		flatten("", x, tmp)
		parts := make([]string, 0, len(tmp))
		for k, v := range tmp {
			parts = append(parts, k+":"+fmt.Sprintf("%v", v))
		}
		return strings.Join(parts, "|")
	default:
		return fmt.Sprintf("%v", x)
	}
}

func normalizeFieldValue(v interface{}) (interface{}, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case uint64:
		return float64(x), true
	case int32:
		return float64(x), true
	case uint32:
		return float64(x), true
	case int16:
		return float64(x), true
	case uint16:
		return float64(x), true
	case int8:
		return float64(x), true
	case uint8:
		return float64(x), true
	case bool:
		return x, true
	case string:
		return x, true
	case time.Time:
		return x.UTC().Format(time.RFC3339Nano), true
	default:
		return nil, false
	}
}

var fieldKeyRe = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func sanitizeFieldKey(k string) string {
	k = strings.TrimSpace(k)
	k = strings.ReplaceAll(k, " ", "_")
	k = fieldKeyRe.ReplaceAllString(k, "_")
	k = strings.Trim(k, "_")
	if k == "" {
		return "field"
	}
	return k
}
