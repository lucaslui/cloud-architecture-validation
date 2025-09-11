package main

import (
	"context"
	"log"
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
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("[error] config error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime.SetupGracefulShutdown(cancel, cfg.Logger)

	if err := broker.EnsureKafkaTopics(ctx, cfg); err != nil {
		cfg.Logger.Fatalf("kafka ensure topics error: %v", err)
	}

	producer := broker.NewKafkaProducer(cfg)
	defer producer.Close(ctx)

	disp := broker.NewKafkaDispatcher(producer, cfg.DispatcherCapacity, cfg.DispatcherMaxBatch, time.Duration(cfg.DispatcherTickMs)*time.Millisecond)
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
				handler.HandleMessage(ctx, cfg, producer, disp, msg)
			}
		}
	}
	for i := 0; i < cfg.ProcessingWorkers; i++ {
		wg.Add(1)
		go workerFn(i)
	}

	cfg.Logger.Printf("[boot] collector workers=%d queue=%d", cfg.ProcessingWorkers, cfg.WorkerQueueSize)

	// Cliente MQTT + reconexão com backoff (bloqueia até conectar)
	client := mqtt.BuildMQTTClient(cfg, producer, disp, workCh)
	mqtt.ConnectWithBackoff(ctx, cfg, client, 2*time.Second, 30*time.Second)

	<-ctx.Done()
	client.Disconnect(250)
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(5 * time.Second):
		cfg.Logger.Println("timeout waiting workers — continuing shutdown")
	}
	cfg.Logger.Println("collector stopped")
}
