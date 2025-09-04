package main

import (
	"context"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Prepara cliente MQTT com callbacks e opções
func buildMQTTClient(cfg *Config, producer *KafkaProducer) mqtt.Client {
	h := func(_ mqtt.Client, msg mqtt.Message) {
		// Contexto simples para o handler; se preferir pode propagar o ctx da main
		handleMessage(context.Background(), cfg, producer, msg)
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.MQTTBrokerURL).
		SetClientID(cfg.MQTTClientID).
		SetOrderMatters(false). // mais concorrência no handler
		SetCleanSession(false). // mantém sessão para re-subscribe automático
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second)

	if cfg.MQTTUsername != "" {
		opts.SetUsername(cfg.MQTTUsername)
	}
	if cfg.MQTTPassword != "" {
		opts.SetPassword(cfg.MQTTPassword)
	}

	// Ao conectar (ou reconectar), refaz a inscrição
	opts.OnConnect = func(c mqtt.Client) {
		cfg.Logger.Printf("connected to VerneMQ: %s", cfg.MQTTBrokerURL)
		if token := c.Subscribe(cfg.MQTTTopic, cfg.MQTTQoS, h); token.Wait() && token.Error() != nil {
			cfg.Logger.Printf("mqtt subscribe error: %v", token.Error())
		} else {
			cfg.Logger.Printf("subscribed to topic: %s (QoS %d)", cfg.MQTTTopic, cfg.MQTTQoS)
		}
	}
	opts.OnConnectionLost = func(c mqtt.Client, err error) { cfg.Logger.Printf("mqtt connection lost: %v", err) }

	return mqtt.NewClient(opts)
}

// Conecta com backoff exponencial até sucesso ou cancel
func connectWithBackoff(ctx context.Context, cfg *Config, client mqtt.Client, start, max time.Duration) {
	backoff := start
	for {
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			cfg.Logger.Printf("mqtt connect error: %v; retrying in %s", token.Error(), backoff)
			select {
			case <-time.After(backoff):
				if backoff < max {
					backoff *= 2
				}
			case <-ctx.Done():
				cfg.Logger.Println("context cancelled before mqtt connect")
				return
			}
			continue
		}
		break
	}
}
