package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type cfg struct {
	RedisAddr           string
	RedisPassword       string
	RedisDB             int
	AuthPassword        string
	TelemetryTopic      string
	CommandsTopicPrefix string
	StartID             int
	EndID               int
	BatchSize           int
	PublishInvalidation bool
}

func main() {
	c := loadConfig()

	if c.AuthPassword == "" || c.TelemetryTopic == "" || c.CommandsTopicPrefix == "" {
		log.Fatalf("AUTH_PASSWORD, TELEMETRY_TOPIC e COMMANDS_TOPIC_PREFIX são obrigatórios")
	}
	if c.StartID < 1 || c.EndID < c.StartID {
		log.Fatalf("intervalo inválido: START_ID=%d END_ID=%d", c.StartID, c.EndID)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     c.RedisAddr,
		Password: c.RedisPassword,
		DB:       c.RedisDB,
	})
	ctx := context.Background()

	passHash := sha256Hex(c.AuthPassword)
	log.Printf("[init] Redis=%s DB=%d range=CONTROLLER-%d..%d batch=%d",
		c.RedisAddr, c.RedisDB, c.StartID, c.EndID, c.BatchSize)

	start := time.Now()
	written := 0

	for base := c.StartID; base <= c.EndID; base += c.BatchSize {
		to := base + c.BatchSize - 1
		if to > c.EndID {
			to = c.EndID
		}

		pipe := rdb.Pipeline()
		for i := base; i <= to; i++ {
			controllerID := fmt.Sprintf("CONTROLLER-%d", i)

			// credenciais
			authKey := "vmq:auth:" + controllerID
			pipe.HSet(ctx, authKey, "password", passHash)

			// ACL publish (telemetria comum)
			aclPubKey := "vmq:acl:pub:" + controllerID
			pipe.SAdd(ctx, aclPubKey, c.TelemetryTopic)

			// ACL subscribe (comandos exclusivos)
			aclSubKey := "vmq:acl:sub:" + controllerID
			pipe.SAdd(ctx, aclSubKey, fmt.Sprintf("%s/%s", c.CommandsTopicPrefix, controllerID))
		}

		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("[err] pipeline [%d..%d]: %v", base, to, err)
		}
		written += (to - base + 1)
		if (to-c.StartID+1)%(c.BatchSize*10) == 0 || to == c.EndID {
			log.Printf("[ok] escritos %d controllers até %s", written, fmt.Sprintf("CONTROLLER-%d", to))
		}
	}

	if c.PublishInvalidation {
		// Canal opcional para avisar serviços a recarregar caches de auth/acl, se usarem
		_ = rdb.Publish(ctx, "auth-acl:invalidate", "ALL").Err()
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[done] total=%d em %s", written, elapsed)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func loadConfig() cfg {
	return cfg{
		RedisAddr:           env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:       os.Getenv("REDIS_PASSWORD"),
		RedisDB:             envInt("REDIS_DB", 0),
		AuthPassword:        os.Getenv("AUTH_PASSWORD"),
		TelemetryTopic:      os.Getenv("TELEMETRY_TOPIC"),
		CommandsTopicPrefix: os.Getenv("COMMANDS_TOPIC_PREFIX"),
		StartID:             envInt("START_ID", 1),
		EndID:               envInt("END_ID", 100000),
		BatchSize:           envInt("BATCH_SIZE", 1000),
		PublishInvalidation: envBool("PUBLISH_INVALIDATION", true),
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
		// fallback: tentar JSON numérico (como no seu loader de schemas)
		var j int
		if err := json.Unmarshal([]byte(v), &j); err == nil && j != 0 {
			return j
		}
	}
	return def
}

func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		switch v {
		case "1", "true", "TRUE", "True", "yes", "Y", "y":
			return true
		case "0", "false", "FALSE", "False", "no", "N", "n":
			return false
		}
	}
	return def
}
