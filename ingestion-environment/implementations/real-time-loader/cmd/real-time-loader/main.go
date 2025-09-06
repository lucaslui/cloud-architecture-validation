package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lucaslui/hems/real-time-loader/internal/broker"
	"github.com/lucaslui/hems/real-time-loader/internal/config"
	"github.com/lucaslui/hems/real-time-loader/internal/database"
	"github.com/lucaslui/hems/real-time-loader/internal/model"
)

func main() {
	cfg := config.LoadEnvVariables()

	log.Printf("[boot] rt-loader | brokers=%s group=%s topic=%s influx=%s org=%s bucket=%s meas_template=%s",
		cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaInputTopic, cfg.InfluxURL, cfg.InfluxOrg, cfg.InfluxBucket, cfg.InfluxMeasurementTemplate)

	db := database.NewInfluxDB(cfg)
	defer db.Close()

	kc := broker.NewKafkaClient(cfg.KafkaBrokers, cfg.KafkaGroupID, cfg.KafkaInputTopic)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setupGracefulShutdown(cancel, kc, db)

	for {
		msg, err := kc.ConsumeMessages(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[kafka] erro ao buscar mensagem: %v", err)
			continue
		}

		var evt model.EnrichedEvent
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("[parse] erro: %v | payload=%s", err, config.Truncate(msg.Value, 512))
			_ = kc.CommitMessages(ctx, msg)
			continue
		}

		if err := db.WriteEvent(ctx, &evt); err != nil {
			log.Printf("[influx] falha ao gravar: %v | device=%s eventId=%s", err, evt.DeviceID, evt.Metadata.EventID)
		} else {
			log.Printf("[influx] ok device=%s ts=%s eventId=%s",
				evt.DeviceID, evt.Timestamp.Format(time.RFC3339), evt.Metadata.EventID)
		}

		if err := kc.CommitMessages(ctx, msg); err != nil {
			log.Printf("[kafka] commit falhou: %v", err)
		}
	}
}

func setupGracefulShutdown(cancel context.CancelFunc, k *broker.KafkaClient, db *database.InfluxDB) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("[shutdown] sinal recebido, finalizando...")
		cancel()
		_ = k.Close()
		db.Close()
	}()
}
