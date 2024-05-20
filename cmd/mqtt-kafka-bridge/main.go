package main

import (
	"os"
	"time"

	"github.com/GirishBhutiya/mqtt-kafka-bridge/cmd/mqtt-kafka-bridge/kafka_processor"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/cmd/mqtt-kafka-bridge/message"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/cmd/mqtt-kafka-bridge/mqtt_processor"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/config"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/internal"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/pkg/kafka/shared"
	"github.com/united-manufacturing-hub/umh-utils/env"
	"github.com/united-manufacturing-hub/umh-utils/logger"
	"go.uber.org/zap"
)

func main() {

	os.Setenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
	os.Setenv("KAFKA_LISTEN_TOPIC", "benthos")
	os.Setenv("MQTT_CERTIFICATE_NAME", "NO_CERT")
	os.Setenv("MQTT_BROKER_URL", "localhost:1883")
	os.Setenv("MQTT_TOPIC", "benthos")
	os.Setenv("MY_POD_NAME", "benthos")

	config, err := config.LoadConfig()
	if err != nil {
		panic(err)
	}

	//var err error
	// Initialize zap logging
	logLevel, _ := env.GetAsString("LOGGING_LEVEL", false, "PRODUCTION") //nolint:errcheck
	log := logger.New(logLevel)
	defer func(logger *zap.SugaredLogger) {
		err = logger.Sync()
		if err != nil {
			panic(err)
		}
	}(log)

	internal.Initfgtrace()

	var mqttToKafkaChan = make(chan shared.KafkaMessage, 100)
	var kafkaToMqttChan = make(chan shared.KafkaMessage, 100)
	var shutdownChan = make(chan bool, 1)

	message.Init()

	kafka_processor.Init(kafkaToMqttChan, shutdownChan, config)
	mqtt_processor.Init(mqttToKafkaChan, shutdownChan, config)

	kafka_processor.Start(mqttToKafkaChan)
	mqtt_processor.Start(kafkaToMqttChan)

	go checkDisconnect(shutdownChan)
	reportStats(shutdownChan, mqttToKafkaChan, kafkaToMqttChan)

	zap.S().Info("Shutting down")
	kafka_processor.Shutdown()
	mqtt_processor.Shutdown()
}

func checkDisconnect(shutdownChan chan bool) {
	var kafkaSent, kafkaRecv, mqttSent, mqttRecv uint64
	kafkaSent, kafkaRecv = kafka_processor.GetStats()
	mqttSent, mqttRecv = mqtt_processor.GetStats()

	for {
		time.Sleep(3 * time.Minute)
		newKafkaSent, newKafkaRecv := kafka_processor.GetStats()
		newMqttSent, newMqttRecv := mqtt_processor.GetStats()
		if newMqttSent == mqttSent && newMqttRecv == mqttRecv {
			zap.S().Error("MQTT connection lost")
			shutdownChan <- true
			return
		}
		if newKafkaSent == kafkaSent && newKafkaRecv == kafkaRecv {
			zap.S().Error("Kafka connection lost")
			shutdownChan <- true
			return
		}
		kafkaSent = newKafkaSent
		kafkaRecv = newKafkaRecv
		mqttSent = newMqttSent
		mqttRecv = newMqttRecv
	}
}

func reportStats(shutdownChan chan bool, mqttToKafkaChan chan shared.KafkaMessage, kafkaToMqttChan chan shared.KafkaMessage) {
	var kafkaSent, kafkaRecv, mqttSent, mqttRecv uint64
	kafkaSent, kafkaRecv = kafka_processor.GetStats()
	mqttSent, mqttRecv = mqtt_processor.GetStats()
	for {
		select {
		case <-shutdownChan:
			return
		case <-time.After(10 * time.Second):
			// Calculate per second
			newKafkaSent, newKafkaRecv := kafka_processor.GetStats()
			newMqttSent, newMqttRecv := mqtt_processor.GetStats()

			kafkaSentPerSecond := (newKafkaSent - kafkaSent) / 10
			kafkaRecvPerSecond := (newKafkaRecv - kafkaRecv) / 10
			mqttSentPerSecond := (newMqttSent - mqttSent) / 10
			mqttRecvPerSecond := (newMqttRecv - mqttRecv) / 10
			cacheUsedRaw, cacheMaxRaw, cacheUsed, cacheMax := message.GetCacheSize()
			cachePercentRaw := float64(cacheUsedRaw) / float64(cacheMaxRaw) * 100
			cachePercent := float64(cacheUsed) / float64(cacheMax) * 100
			zap.S().Infof(
				"Kafka sent: %d (%d/s), Kafka recv: %d (%d/s) | MQTT sent: %d (%d/s), MQTT recv: %d (%d/s) | Cached: %d/%d (%.2f%%), Cached raw: %d/%d (%.2f%%) | MqttToKafka chan: %d | KafkaToMqtt chan: %d",
				newKafkaSent, kafkaSentPerSecond, newKafkaRecv, kafkaRecvPerSecond, newMqttSent, mqttSentPerSecond, newMqttRecv, mqttRecvPerSecond, cacheUsed, cacheMax, cachePercent, cacheUsedRaw, cacheMaxRaw, cachePercentRaw, len(mqttToKafkaChan), len(kafkaToMqttChan))

			kafkaSent = newKafkaSent
			kafkaRecv = newKafkaRecv
			mqttSent = newMqttSent
			mqttRecv = newMqttRecv
		}
	}
}
