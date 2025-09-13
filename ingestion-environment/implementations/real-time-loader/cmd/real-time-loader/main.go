// cmd/main.go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/lucaslui/hems/real-time-loader/internal/broker"
	"github.com/lucaslui/hems/real-time-loader/internal/config"
	"github.com/lucaslui/hems/real-time-loader/internal/database"
	"github.com/lucaslui/hems/real-time-loader/internal/model"
)

func main() {
	logger := config.GetLogger()

	cfg, err := config.LoadConfig(logger)
	if err != nil {
		logger.Fatalf("[boot] configuração inválida: %v", err)
	}

	logger.Printf("[info] real-time loader configs loaded:%s", cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setupGracefulShutdown(cancel, logger)

	if err := broker.EnsureKafkaTopics(ctx, cfg, logger); err != nil {
		logger.Fatalf("[error] kafka ensure topics error: %v", err)
	}

	kc := broker.NewKafkaClient(cfg)
	defer kc.Close()

	msgCh := make(chan kafka.Message, 5000)
	ackCh := make(chan kafka.Message, 5000)

	// 1) Fetcher: lê o mais rápido possível (FetchMessage já é eficiente)
	go func() {
		for {
			m, err := kc.ConsumeMessages(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return
				}
				logger.Printf("[kafka] erro ao buscar: %v", err)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			msgCh <- m
		}
	}()

	db := database.NewInfluxDB(cfg)
	defer db.Close()

	// 2) Workers: parse → enfileira no Influx (WriteAPI assíncrono) → sinaliza ack
	var wg sync.WaitGroup
	wg.Add(cfg.ProcessingWorkers)
	// fetcher: igual ao que já te passei (kc.ConsumeMessages -> msgCh)
	// workers: agora ack após ENFILEIRAR no Influx
	for i := 0; i < cfg.ProcessingWorkers; i++ {
		go func(id int) {
			defer wg.Done()
			for m := range msgCh {
				var evt model.InboundEnvelope
				if err := json.Unmarshal(m.Value, &evt); err != nil {
					logger.Printf("[parse] erro: %v | payload=%s", err, config.Truncate(m.Value, 256))
					ackCh <- m // descarta inválidas para não travar a partição
					continue
				}
				// enqueue no WriteAPI assíncrono
				if err := db.WriteEvent(ctx, &evt); err != nil {
					// raríssimo aqui (só erro local de point). Não ACK → reprocessa
					logger.Printf("[influx-async] enqueue falhou: %v", err)
					continue
				}
				// sucesso no ENFILEIRAMENTO → ACK
				ackCh <- m
			}
		}(i)
	}

	// committer: commit em lote (p.ex. 500 msgs ou 100ms)
	go func() {
		batch := make([]kafka.Message, 0, cfg.KafkaAckBatchSize)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if err := kc.CommitMessages(context.Background(), batch...); err != nil {
				logger.Printf("[kafka] commit falhou: %v", err)
			}
			batch = batch[:0]
		}
		for {
			select {
			case m, ok := <-ackCh:
				if !ok {
					flush()
					return
				}
				batch = append(batch, m)
				if len(batch) >= cfg.KafkaAckBatchSize {
					flush()
				}
			case <-t.C:
				flush()
			}
		}
	}()

	<-ctx.Done()
	close(msgCh)
	wg.Wait()
	close(ackCh)
}

func setupGracefulShutdown(cancel context.CancelFunc, logger *log.Logger) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		logger.Printf("[shutdown] sinal recebido, finalizando...")
		cancel()
	}()
}
