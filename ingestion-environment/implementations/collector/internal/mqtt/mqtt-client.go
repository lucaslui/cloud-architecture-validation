package mqtt

import (
	"context"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/lucaslui/hems/collector/internal/config"
)

func BuildMQTTClient(cfg *config.Config, logger *log.Logger, sink chan<- mqtt.Message) mqtt.Client {
	h := func(_ mqtt.Client, msg mqtt.Message) {
		if cfg.WorkQueueStrategy == "block" && cfg.WorkQueueEnqTimeoutMs > 0 {
			timer := time.NewTimer(time.Duration(cfg.WorkQueueEnqTimeoutMs) * time.Millisecond)
			defer timer.Stop()
			select {
			case sink <- msg:
			case <-timer.C:
				logger.Printf("work queue timeout — dropping: topic=%s mid=%d", msg.Topic(), msg.MessageID())
			}
			return
		}
		select {
		case sink <- msg:
		default:
			logger.Printf("work queue full — dropping: topic=%s mid=%d", msg.Topic(), msg.MessageID())
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBrokerURL).
		SetClientID(cfg.MQTTClientID).
		SetOrderMatters(false).
		SetCleanSession(false).
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetMessageChannelDepth(cfg.MQTTChannelDepth)

	if cfg.MQTTUsername != "" {
		opts.SetUsername(cfg.MQTTUsername)
	}
	if cfg.MQTTPassword != "" {
		opts.SetPassword(cfg.MQTTPassword)
	}

	opts.OnConnect = func(c mqtt.Client) {
		logger.Printf("[info] mqtt client connected to VerneMQ: %s", cfg.MQTTBrokerURL)
		if token := c.Subscribe(cfg.MQTTTopic, cfg.MQTTQoS, h); token.Wait() && token.Error() != nil {
			logger.Printf("[error] mqtt client subscribe error: %v", token.Error())
		} else {
			logger.Printf("[info] mqtt client subscribed to topic: %s (QoS %d)", cfg.MQTTTopic, cfg.MQTTQoS)
		}
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) { logger.Printf("[error] mqtt client connection lost: %v", err) }

	return mqtt.NewClient(opts)
}

func ConnectWithBackoff(ctx context.Context, logger *log.Logger, client mqtt.Client, start, max time.Duration) {
	backoff := start
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			logger.Printf("[error] mqtt connect error: %v; retrying in %s", token.Error(), backoff)
			select {
			case <-time.After(backoff):
				if backoff < max {
					backoff *= 2
				}
			case <-ctx.Done():
				logger.Println("[error] context cancelled before mqtt connect")
				return
			}
			continue
		}
		break
	}
}
