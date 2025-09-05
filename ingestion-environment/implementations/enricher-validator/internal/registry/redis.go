package registry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/santhosh-tekuri/jsonschema/v5"
)

type SchemaRegistry interface {
	Load(deviceType, version, eventType string) (*jsonschema.Schema, string, error)
}

type RedisOpts struct {
	Addr, Password, Namespace, InvalidateChannel string
	DB int
	UsePubSub bool
	Timeout   time.Duration
}

type redisRegistry struct {
	rdb       *redis.Client
	nsPrefix  string
	subject   string
	usePubSub bool
	memCache  sync.Map
}

func NewRedisRegistry(o RedisOpts) SchemaRegistry {
	rdb := redis.NewClient(&redis.Options{
		Addr:         o.Addr,
		Password:     o.Password,
		DB:           o.DB,
		DialTimeout:  o.Timeout,
		ReadTimeout:  o.Timeout,
		WriteTimeout: o.Timeout,
	})
	rr := &redisRegistry{
		rdb:       rdb,
		nsPrefix:  firstNonEmpty(o.Namespace, "schema"),
		subject:   firstNonEmpty(o.InvalidateChannel, "schemas:invalidate"),
		usePubSub: o.UsePubSub,
	}
	if rr.usePubSub {
		go rr.listenInvalidations(context.Background())
	}
	return rr
}

func (r *redisRegistry) key(deviceType, version, eventType string) (string, error) {
	if deviceType == "" || version == "" || eventType == "" {
		return "", errors.New("deviceType/version/eventType vazios")
	}
	return fmt.Sprintf("%s:%s:%s:%s", r.nsPrefix, deviceType, version, eventType), nil
}

func (r *redisRegistry) Load(deviceType, version, eventType string) (*jsonschema.Schema, string, error) {
	k, err := r.key(deviceType, version, eventType); if err != nil { return nil, "", err }
	if v, ok := r.memCache.Load(k); ok { return v.(*jsonschema.Schema), k, nil }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
	val, err := r.rdb.Get(ctx, k).Bytes()
	if err != nil {
		if err == redis.Nil { return nil, "", fmt.Errorf("schema ausente no redis (%s)", k) }
		return nil, "", fmt.Errorf("falha ao buscar schema no redis (%s): %w", k, err)
	}
	if len(val) == 0 { return nil, "", fmt.Errorf("schema vazio no redis (%s)", k) }

	sch, cErr := compileSchema(val, k); if cErr != nil { return nil, "", cErr }
	r.memCache.Store(k, sch)
	return sch, k, nil
}

func (r *redisRegistry) listenInvalidations(ctx context.Context) {
	pubsub := r.rdb.Subscribe(ctx, r.subject)
	for msg := range pubsub.Channel() {
		payload := strings.TrimSpace(msg.Payload)
		if payload == "ALL" || payload == "" {
			r.memCache = sync.Map{}; continue
		}
		r.memCache.Delete(payload)
	}
}

func compileSchema(b []byte, ref string) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	if err := c.AddResource(ref, bytes.NewReader(b)); err != nil { return nil, err }
	return c.Compile(ref)
}

func firstNonEmpty(s, def string) string { if strings.TrimSpace(s) != "" { return s }; return def }
