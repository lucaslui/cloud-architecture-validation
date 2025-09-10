package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaDispatcher despacha mensagens para Kafka em lote, com flush periódico ou quando o lote enche
type KafkaDispatcher struct {
	prod    *KafkaProducer
	inputCh chan kafka.Message
	stopCh  chan struct{}
}

func NewKafkaDispatcher(prod *KafkaProducer, capacity int) *KafkaDispatcher {
	d := &KafkaDispatcher{
		prod:    prod,
		inputCh: make(chan kafka.Message, capacity),
		stopCh:  make(chan struct{}),
	}
	go d.loop()
	return d
}

func (d *KafkaDispatcher) loop() {
	batch := make([]kafka.Message, 0, 2000)
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		_ = d.prod.main.WriteMessages(context.Background(), batch...) // já está Async+batch
		batch = batch[:0]
	}

	for {
		select {
		case m := <-d.inputCh:
			batch = append(batch, m)
			if len(batch) >= 2000 {
				flush()
			}
		case <-t.C:
			flush()
		case <-d.stopCh:
			flush()
			return
		}
	}
}

func (d *KafkaDispatcher) Enqueue(m kafka.Message) {
	select {
	case d.inputCh <- m:
	default:
		// buffer cheio: escolha entre bloquear, dropar ou logar e tentar novamente
		// aqui vamos bloquear brevemente
		d.inputCh <- m
	}
}

func (d *KafkaDispatcher) Stop() { close(d.stopCh) }
