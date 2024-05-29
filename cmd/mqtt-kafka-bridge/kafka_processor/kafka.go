package kafka_processor

import (
	"log"
	"strings"

	l "log"

	"github.com/GirishBhutiya/mqtt-kafka-bridge/cmd/mqtt-kafka-bridge/message"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/config"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/internal"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/pkg/kafka/consumer/redpanda"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/pkg/kafka/producer"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/pkg/kafka/shared"
	"github.com/united-manufacturing-hub/umh-utils/env"
	"go.uber.org/zap"
)

//var client *kafka.Client

type client struct {
	producer *producer.Producer
	consumer *redpanda.Consumer
}

var cl *client

func Init(kafkaToMqttChan chan shared.KafkaMessage, sChan chan bool, conf config.ConfigVars) {

	l.Println("in kafka.Init()")
	tl := config.CreateTlsConfiguration(conf.KafkaCertFilePath, conf.KafkaKeyFilePath, "", true)
	/* KafkaBootstrapServer, err := env.GetAsString("KAFKA_BOOTSTRAP_SERVER", true, "")
	if err != nil {
		zap.S().Fatal(err)
	}
	KafkaTopic, err := env.GetAsString("KAFKA_LISTEN_TOPIC", true, "")
	if err != nil {
		zap.S().Fatal(err)
	}
	useSsl, err := env.GetAsBool("KAFKA_USE_SSL", false, false)
	if err != nil {
		zap.S().Error(err)
	}
	clientid, err := env.GetAsString("KAFKA_CLIENT_ID", false, "")
	if err != nil {
		zap.S().Error(err)
	} */

	/* compile, err := regexp.Compile(KafkaTopic)
	if err != nil {
		zap.S().Fatalf("Error compiling regex: %v", err)
	} */
	//log.Println("tl", tl)
	log.Println("server", conf.KafkaBootstrapServer)
	pr, err := producer.NewProducer(conf.KafkaBootstrapServer, conf.KafkaUsername, conf.KafkaPassword, tl)
	if err != nil {
		zap.S().Fatalf("Error creating kafka producer: %v", err)
	}
	consumr, err := redpanda.NewConsumer(conf.KafkaBootstrapServer, conf.KafkaListenTopic, conf.KafkaClientId, conf.KafkaClientId, conf.KafkaUsername, conf.KafkaPassword, tl)

	cl = &client{
		producer: pr,
		consumer: consumr,
	}
	/* client, err = kafka.NewKafkaClient(&kafka.NewClientOptions{
		Brokers: []string{
			KafkaBootstrapServer,
		},
		ConsumerGroupId:   "mqtt-kafka-bridge",
		ListenTopicRegex:  compile,
		Partitions:        6,
		ReplicationFactor: 1,
		EnableTLS:         useSsl,
		StartOffset:       sarama.OffsetOldest,
	}) */

	if err != nil {
		zap.S().Fatalf("Error creating kafka client: %v", err)
		return
	}
	go processIncomingMessage(kafkaToMqttChan)
}

func processIncomingMessage(kafkaToMqttChan chan shared.KafkaMessage) {
	for {

		msg := <-cl.consumer.GetMessages()
		kafkaToMqttChan <- shared.KafkaMessage{
			Topic:   msg.Topic,
			Value:   msg.Value,
			Headers: msg.Headers,
			Key:     msg.Key,
		}

	}
}

func Shutdown() {
	zap.S().Info("Shutting down kafka client")
	err := cl.producer.Close()

	if err != nil {
		zap.S().Fatalf("Error closing kafka client: %v", err)
	}
	zap.S().Info("Kafka client shut down")
}

func Start(mqttToKafkaChan chan shared.KafkaMessage) {
	KafkaSenderThreads, err := env.GetAsInt("KAFKA_SENDER_THREADS", false, 1)
	if err != nil {
		zap.S().Error(err)
	}
	if KafkaSenderThreads < 1 {
		zap.S().Fatal("KAFKA_SENDER_THREADS must be at least 1")
	}
	for i := 0; i < KafkaSenderThreads; i++ {
		go start(mqttToKafkaChan)
	}
}

func start(mqttToKafkaChan chan shared.KafkaMessage) {
	for {
		msg := <-mqttToKafkaChan

		msg.Topic = strings.ReplaceAll(msg.Topic, "$share/MQTT_KAFKA_BRIDGE/", "")
		if !message.IsValidMQTTMessage(msg.Topic, msg.Value) {
			continue
		}
		// Change MQTT to Kafka topic format
		msg.Topic = strings.ReplaceAll(msg.Topic, "/", ".")

		internal.AddSXOrigin(&msg)
		var err error
		err = internal.AddSXTrace(&msg)
		if err != nil {
			zap.S().Fatalf("Failed to marshal trace")
			continue
		}

		cl.producer.SendMessage(&msg)
		/* for err != nil {
			time.Sleep(10 * time.Millisecond)
			err = client.EnqueueMessage(msg)
		} */
	}
}

func GetStats() (sent, received uint64) {
	return cl.consumer.GetStats()
}
