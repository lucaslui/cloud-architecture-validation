// KafkaDispatcher is responsible for batching and dispatching messages to Kafka.
// It collects messages into a buffer and flushes them either periodically or when the batch reaches a specified size.
// The dispatcher runs in a background goroutine, ensuring efficient and asynchronous message delivery.
//
// Usage:
//   - Create a new dispatcher with NewKafkaDispatcher, providing a KafkaProducer and buffer capacity.
//   - Use Enqueue to add messages to the dispatcher.
//   - Call Stop to gracefully flush remaining messages and stop the dispatcher.
//
// The dispatcher handles buffer overflow by briefly blocking when the input channel is full.
package broker

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaDispatcher struct {
	producer    *KafkaProducer
	inputChannel chan kafka.Message
	stopChannel  chan struct{}
}

func NewKafkaDispatcher(prod *KafkaProducer, capacity int) *KafkaDispatcher {
	d := &KafkaDispatcher{
		producer:    prod,
		inputChannel: make(chan kafka.Message, capacity),
		stopChannel:  make(chan struct{}),
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
		_ = d.producer.main.WriteMessages(context.Background(), batch...) // já está Async+batch
		batch = batch[:0]
	}

	for {
		select {
		case m := <-d.inputChannel:
			batch = append(batch, m)
			if len(batch) >= 2000 {
				flush()
			}
		case <-t.C:
			flush()
		case <-d.stopChannel:
			flush()
			return
		}
	}
}

func (d *KafkaDispatcher) Enqueue(message kafka.Message) {
	select {
	case d.inputChannel <- message:
	default:
		// buffer full: in the future I need to choose between blocking, dropping, or logging and retrying
		// here we will briefly block
		d.inputChannel <- message
	}
}

func (d *KafkaDispatcher) Stop() { close(d.stopChannel) }
