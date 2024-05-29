package producer

import (
	"crypto/tls"
	"log"
	"sync/atomic"
	"time"

	"github.com/GirishBhutiya/mqtt-kafka-bridge/pkg/kafka/shared"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Producer struct wraps a sarama.AsyncProducer and handles Kafka message production.
type Producer struct {
	producer         *sarama.AsyncProducer
	brokers          []string
	producedMessages atomic.Uint64
	erroredMessages  atomic.Uint64
	running          atomic.Bool
}

// NewProducer creates a new Producer with the given Kafka brokers.
func NewProducer(brokers []string, username, password string, tlsConfig *tls.Config) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	if username != "" {
		config.Net.SASL.User = username
		config.Net.SASL.Password = password
	}
	if tlsConfig != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
	}

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Println("NewAsyncProducer", err)
		return nil, err
	}

	p := &Producer{
		brokers:  brokers,
		producer: &producer,
	}
	p.running.Store(true)
	go p.handleErrors()

	return p, nil
}

// handleErrors handles errors from the producer in a goroutine.
func (p *Producer) handleErrors() {
	timeout := time.NewTimer(shared.CycleTime)
	defer timeout.Stop()

	for p.running.Load() {
		select {
		case <-timeout.C:
			timeout.Reset(shared.CycleTime)
		case err := <-(*p.producer).Errors():
			if err != nil {
				p.erroredMessages.Add(1)
				zap.S().Debugf("Error while producing message: %s", err.Error())
			}
		}
	}
}

// SendMessage sends a KafkaMessage to the producer.
func (p *Producer) SendMessage(message *shared.KafkaMessage) {
	if message == nil {
		return
	}
	(*p.producer).Input() <- shared.ToProducerMessage(message)
	p.producedMessages.Add(1)
}

// Close stops the producer and returns any errors during closure.
func (p *Producer) Close() error {
	p.running.Store(false)
	return (*p.producer).Close()
}

// GetProducedMessages returns the count of produced and errored messages.
func (p *Producer) GetProducedMessages() (uint64, uint64) {
	return p.producedMessages.Load(), p.erroredMessages.Load()
}
