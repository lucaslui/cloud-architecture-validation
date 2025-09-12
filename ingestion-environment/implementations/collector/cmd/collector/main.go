package main

import (
	"context"
	"sync"
	"time"

	mqttsdk "github.com/eclipse/paho.mqtt.golang"

	"github.com/lucaslui/hems/collector/internal/broker"
	"github.com/lucaslui/hems/collector/internal/config"
	"github.com/lucaslui/hems/collector/internal/handler"
	"github.com/lucaslui/hems/collector/internal/mqtt"
	"github.com/lucaslui/hems/collector/internal/runtime"
)

func main() {
	logger := config.GetLogger()

	cfg, err := config.LoadConfig()
	if err != nil {
		logger.Fatalf("[error] configuração inválida: %v", err)
	}

	logger.Printf("[info] collector configs loaded:%s", cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime.SetupGracefulShutdown(cancel, logger)

	if err := broker.EnsureKafkaTopics(ctx, cfg, logger); err != nil {
		logger.Fatalf("[error] kafka ensure topics error: %v", err)
	}

	kafkaClient := broker.NewKafkaClient(cfg)
	defer kafkaClient.Close()

	disp := broker.NewKafkaDispatcher(kafkaClient, cfg.DispatcherCapacity, cfg.DispatcherMaxBatch, time.Duration(cfg.DispatcherTickMs)*time.Millisecond)
	defer disp.Stop()

	// ===== Pool de workers para processar mensagens MQTT =====
	workCh := make(chan mqttsdk.Message, cfg.WorkerQueueSize)
	var wg sync.WaitGroup
	workerFn := func(_ int) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-workCh:
				handler.HandleMessage(ctx, cfg, logger, kafkaClient, disp, msg)
			}
		}
	}
	for i := 0; i < cfg.ProcessingWorkers; i++ {
		wg.Add(1)
		go workerFn(i)
	}

	// Cliente MQTT + reconexão com backoff (bloqueia até conectar)
	client := mqtt.BuildMQTTClient(cfg, logger, workCh)
	mqtt.ConnectWithBackoff(ctx, logger, client, 2*time.Second, 30*time.Second)

	<-ctx.Done()
	client.Disconnect(250)
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		logger.Println("[error] timeout waiting workers — continuing shutdown")
	}
	logger.Println("[info] collector stopped")
}
