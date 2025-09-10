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
	cfg := config.LoadEnvVariables()

	log.Printf("[boot] rt-loader | brokers=%s group=%s topic=%s influx=%s org=%s bucket=%s",
		cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaInputTopic, cfg.InfluxURL, cfg.InfluxOrg, cfg.InfluxBucket)

	db := database.NewInfluxDB(cfg)
	defer db.Close()

	kc := broker.NewKafkaClient(cfg)
	defer kc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setupGracefulShutdown(cancel, kc, db)

	// Canais
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
				log.Printf("[kafka] erro ao buscar: %v", err)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			msgCh <- m
		}
	}()

	// 2) Workers: parse → enfileira no Influx (WriteAPI assíncrono) → sinaliza ack
	var wg sync.WaitGroup
	wg.Add(cfg.Workers)
	// fetcher: igual ao que já te passei (kc.ConsumeMessages -> msgCh)
	// workers: agora ack após ENFILEIRAR no Influx
	for i := 0; i < cfg.Workers; i++ {
		go func(id int) {
			defer wg.Done()
			for m := range msgCh {
				var evt model.InboundEnvelope
				if err := json.Unmarshal(m.Value, &evt); err != nil {
					log.Printf("[parse] erro: %v | payload=%s", err, config.Truncate(m.Value, 256))
					ackCh <- m // descarta inválidas para não travar a partição
					continue
				}
				// enqueue no WriteAPI assíncrono
				if err := db.WriteEvent(ctx, &evt); err != nil {
					// raríssimo aqui (só erro local de point). Não ACK → reprocessa
					log.Printf("[influx-async] enqueue falhou: %v", err)
					continue
				}
				// sucesso no ENFILEIRAMENTO → ACK
				ackCh <- m
			}
		}(i)
	}

	// committer: commit em lote (p.ex. 500 msgs ou 100ms)
	go func() {
		batch := make([]kafka.Message, 0, cfg.AckBatchSize)
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if err := kc.CommitMessages(context.Background(), batch...); err != nil {
				log.Printf("[kafka] commit falhou: %v", err)
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
				if len(batch) >= cfg.AckBatchSize {
					flush()
				}
			case <-t.C:
				flush()
			}
		}
	}()

	// 3) Espera término
	<-ctx.Done()
	close(msgCh)
	wg.Wait()
	close(ackCh)
}

func setupGracefulShutdown(cancel context.CancelFunc, k *broker.KafkaClient, db *database.InfluxDB) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("[shutdown] sinal recebido, finalizando...")
		cancel()
	}()
}
