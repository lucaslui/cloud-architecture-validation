package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
)

type cfg struct {
	RedisAddr     string
	RedisPassword string
	RedisDB       int

	// Credencial comum dos controllers (será hasheada em bcrypt)
	AuthPassword string

	// ACLs
	TelemetryTopic      string // tópico comum de telemetria (publish dos controllers / subscribe do coletor)
	CommandsTopicPrefix string // prefixo de comandos; cada controller assina "<prefix>/<controllerId>"

	// Range de controllers
	StartID   int
	EndID     int
	BatchSize int

	// Pub opcional para invalidação de caches (se tiver consumidores)
	PublishInvalidation bool

	// Usuário do coletor
	CollectorUsername string
	CollectorPassword string

	// Mountpoint (normalmente vazio)
	Mountpoint string
}

type aclPattern struct {
	Pattern string `json:"pattern"`
}

type record struct {
	Passhash     string       `json:"passhash"`
	SubscribeACL []aclPattern `json:"subscribe_acl,omitempty"`
	PublishACL   []aclPattern `json:"publish_acl,omitempty"`
}

func main() {
	c := loadConfig()

	// validações básicas
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

	passHash := bcryptHash(c.AuthPassword)

	log.Printf("[init] Redis=%s DB=%d range=CONTROLLER-%d..%d batch=%d",
		c.RedisAddr, c.RedisDB, c.StartID, c.EndID, c.BatchSize)

	start := time.Now()
	written := 0

	// Controllers
	for base := c.StartID; base <= c.EndID; base += c.BatchSize {
		to := base + c.BatchSize - 1
		if to > c.EndID {
			to = c.EndID
		}

		pipe := rdb.Pipeline()
		for i := base; i <= to; i++ {
			controllerID := fmt.Sprintf("CONTROLLER-%d", i)
			upsertController(ctx, pipe, c.Mountpoint, controllerID, passHash, c.TelemetryTopic, c.CommandsTopicPrefix)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("[err] pipeline [%d..%d]: %v", base, to, err)
		}

		written += (to - base + 1)
		if (to-c.StartID+1)%(c.BatchSize*10) == 0 || to == c.EndID {
			log.Printf("[ok] escritos %d controllers até %s", written, fmt.Sprintf("CONTROLLER-%d", to))
		}
	}

	// Coletor
	if c.CollectorUsername != "" && c.CollectorPassword != "" {
		colPassHash := bcryptHash(c.CollectorPassword)
		pipe := rdb.Pipeline()
		upsertCollector(ctx, pipe, c.Mountpoint, c.CollectorUsername, colPassHash, c.TelemetryTopic)
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("[err] collector setup: %v", err)
		}
		log.Printf("[ok] collector criado: %s (sub: %q)", c.CollectorUsername, c.TelemetryTopic)
	} else {
		log.Printf("[warn] COLLECTOR_USERNAME/COLLECTOR_PASSWORD não definidos — pulando criação do coletor")
	}

	// Notificação opcional
	if c.PublishInvalidation {
		_ = rdb.Publish(ctx, "auth-acl:invalidate", "ALL").Err()
	}

	elapsed := time.Since(start).Truncate(time.Millisecond)
	log.Printf("[done] total=%d em %s", written, elapsed)
}

// ===== helpers =====

func upsertController(ctx context.Context, pipe redis.Pipeliner, mountpoint, controllerID, bcryptHash, telemetryTopic, commandsPrefix string) {
	key := redisKey(mountpoint, controllerID, controllerID)
	val := record{
		Passhash:     bcryptHash,
		PublishACL:   []aclPattern{{Pattern: telemetryTopic}},
		SubscribeACL: []aclPattern{{Pattern: fmt.Sprintf("%s/%s", commandsPrefix, controllerID)}},
	}
	b, _ := json.Marshal(val)
	pipe.Set(ctx, key, string(b), 0)
}

func upsertCollector(ctx context.Context, pipe redis.Pipeliner, mountpoint, username, bcryptHash, telemetryTopic string) {
	// por simplicidade, client_id = username
	key := redisKey(mountpoint, username, username)
	val := record{
		Passhash:     bcryptHash,
		SubscribeACL: []aclPattern{{Pattern: telemetryTopic}},
	}
	b, _ := json.Marshal(val)
	pipe.Set(ctx, key, string(b), 0)
}

func redisKey(mountpoint, clientID, username string) string {
	// chave EXATAMENTE como a doc: ["<mount>","<client_id>","<username>"] sem espaços
	return fmt.Sprintf("[\"%s\",\"%s\",\"%s\"]", mountpoint, clientID, username)
}

func bcryptHash(s string) string {
	// use 12 rounds (mais comum com vmq_bcrypt.default_log_rounds=12)
	const cost = 12
	hash, err := bcrypt.GenerateFromPassword([]byte(s), cost)
	if err != nil {
		log.Fatalf("erro ao gerar bcrypt: %v", err)
	}
	return string(hash)
}

func loadConfig() cfg {
	return cfg{
		RedisAddr:     env("REDIS_ADDR", "localhost:6379"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisDB:       envInt("REDIS_DB", 0),

		AuthPassword:        os.Getenv("AUTH_PASSWORD"),
		TelemetryTopic:      os.Getenv("TELEMETRY_TOPIC"),
		CommandsTopicPrefix: os.Getenv("COMMANDS_TOPIC_PREFIX"),

		StartID:   envInt("START_ID", 1),
		EndID:     envInt("END_ID", 100000),
		BatchSize: envInt("BATCH_SIZE", 1000),

		PublishInvalidation: envBool("PUBLISH_INVALIDATION", true),

		CollectorUsername: os.Getenv("COLLECTOR_USERNAME"),
		CollectorPassword: os.Getenv("COLLECTOR_PASSWORD"),

		Mountpoint: env("MOUNTPOINT", ""), // normalmente vazio
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
		var j int
		if err := json.Unmarshal([]byte(v), &j); err == nil {
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
