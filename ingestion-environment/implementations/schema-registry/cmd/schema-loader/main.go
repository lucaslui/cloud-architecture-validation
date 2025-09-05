package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultFSRoot       = "../schema-registry"
	schemasPerDeviceDir = "schemas"
)

func main() {
	fsRoot := env("SCHEMA_REGISTRY_FS_ROOT", defaultFSRoot)
	addr := env("REDIS_ADDR", "localhost:6379")
	pass := os.Getenv("REDIS_PASSWORD")
	db := envInt("REDIS_DB", 0)
	ttl := envInt("SCHEMA_TTL_SECONDS", 0)

	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: pass, DB: db})
	ctx := context.Background()

	root := filepath.Join(fsRoot, schemasPerDeviceDir)
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil { return err }
		if info.IsDir() { return nil }
		if !strings.HasSuffix(info.Name(), ".json") { return nil }

		// path: .../<DeviceType>/<Version>/<EventType>.json
		rel, _ := filepath.Rel(root, path)
		parts := strings.Split(rel, string(os.PathSeparator))
		if len(parts) != 3 { return nil }

		deviceType := parts[0]
		version := parts[1]
		eventType := strings.TrimSuffix(parts[2], ".json")

		b, readErr := os.ReadFile(path)
		if readErr != nil { return readErr }

		k := fmt.Sprintf("schema:%s:%s:%s", deviceType, version, eventType)
		h := sha256.Sum256(b)
		hash := hex.EncodeToString(h[:])

		if ttl > 0 {
			if err := rdb.Set(ctx, k, b, time.Duration(ttl)*time.Second).Err(); err != nil { return err }
		} else {
			if err := rdb.Set(ctx, k, b, 0).Err(); err != nil { return err }
		}

		metaKey := "schema:meta:" + deviceType + ":" + version + ":" + eventType
		meta := map[string]any{
			"hash":        hash,
			"updatedAt":   time.Now().UTC().Format(time.RFC3339),
			"contentType": "application/json",
		}
		if err := rdb.HSet(ctx, metaKey, meta).Err(); err != nil { return err }

		// (Opcional) index por deviceType
		idxKey := "schema:index:" + deviceType
		if err := rdb.SAdd(ctx, idxKey, k).Err(); err != nil { return err }

		log.Printf("[ok] %s -> %s", path, k)
		return nil
	})
	if err != nil { log.Fatal(err) }

	// Publica invalidação global (consumidores podem optar por recarregar caches)
	if err := rdb.Publish(ctx, "schemas:invalidate", "ALL").Err(); err != nil {
		log.Printf("[warn] publish invalidate: %v", err)
	}
}

func env(key, def string) string { if v := os.Getenv(key); v != "" { return v }; return def }
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var i int
		_ = json.Unmarshal([]byte(v), &i) // simples; poderíamos usar strconv.Atoi
		if i != 0 { return i }
	}
	return def
}
