package mqtt_processor

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/GirishBhutiya/mqtt-kafka-bridge/cmd/mqtt-kafka-bridge/message"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/config"
	"github.com/GirishBhutiya/mqtt-kafka-bridge/pkg/kafka/shared"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/heptiolabs/healthcheck"
	"github.com/united-manufacturing-hub/umh-utils/env"
	"go.uber.org/zap"
)

var mqttClient MQTT.Client
var sChan chan bool

var sentMessages atomic.Uint64
var receivedMessages atomic.Uint64

func Init(mqttToKafkaChan chan shared.KafkaMessage, shutdownChan chan bool, config config.ConfigVars) {
	if mqttClient != nil {
		return
	}
	sChan = shutdownChan
	/* certificateName, err := env.GetAsString("MQTT_CERTIFICATE_NAME", true, "")
	if err != nil {
		zap.S().Fatal(err)
	}
	mqttBrokerURL, err := env.GetAsString("MQTT_BROKER_URL", true, "")
	if err != nil {
		zap.S().Fatal(err)
	}
	mqttTopic, err := env.GetAsString("MQTT_TOPIC", true, "")
	if err != nil {
		zap.S().Fatal(err)
	}
	podName, err := env.GetAsString("MY_POD_NAME", true, "")
	if err != nil {
		zap.S().Fatal(err)
	}
	username, err := env.GetAsString("MQTT_USERNAME", false, "")
	if err != nil {
		zap.S().Error(err)
	}
	password, err := env.GetAsString("MQTT_PASSWORD", false, "")
	if err != nil {
		zap.S().Error(err)
	} */

	opts := MQTT.NewClientOptions()
	opts.AddBroker(config.MQTTBrokerURL)
	opts.SetUsername(config.MQTTUsername)
	if config.MQTTPassword != "" {
		opts.SetPassword(config.MQTTPassword)
	}

	// Check if topic is using $share
	if strings.Index(config.MQTTTopic, "$share") != 0 {
		// Add $share/MQTT_KAFKA_BRIDGE/ to topic
		config.MQTTTopic = "$share/MQTT_KAFKA_BRIDGE/" + config.MQTTTopic
	}

	if config.MQTTCertName == "NO_CERT" {
		opts.SetClientID(config.MyPodName)

		zap.S().Infof("Running in Kubernetes mode (%s) (%s)", config.MyPodName, config.MQTTTopic)

	} else {
		tlsconfig := newTLSConfig()
		opts.SetClientID(config.MyPodName).SetTLSConfig(tlsconfig)

		zap.S().Infof("Running in normal mode (%s) (%s) (%s)", config.MQTTTopic, config.MQTTCertName, config.MyPodName)

	}
	opts.SetAutoReconnect(true)
	opts.SetOnConnectHandler(onConnect)
	opts.SetConnectionLostHandler(onConnectionLost)
	opts.SetOrderMatters(false)

	zap.S().Debugf("Broker configured (%s) (%s) (%s)", config.MQTTBrokerURL, config.MQTTCertName, config.MyPodName)

	// Start the connection
	mqttClient = MQTT.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		zap.S().Fatalf("Failed to connect: %s", token.Error())
	}

	// Subscribe
	if token := mqttClient.Subscribe(config.MQTTTopic, 1, getOnMessageReceived(mqttToKafkaChan)); token.Wait() && token.Error() != nil {
		zap.S().Fatalf("Failed to subscribe: %s", token.Error())
	}
	zap.S().Infof("MQTT subscribed (%s)", config.MQTTTopic)

	health := healthcheck.NewHandler()
	health.AddLivenessCheck("goroutine-threshold", healthcheck.GoroutineCountCheck(1000000))
	go func() {
		/* #nosec G114 */
		err := http.ListenAndServe("0.0.0.0:8086", health)
		if err != nil {
			zap.S().Fatalf("Error starting healthcheck %v", err)
		}
	}()

	// Implement a custom check with a 50 millisecond timeout.
	health.AddReadinessCheck("mqtt-check", checkConnected(mqttClient))
}

func checkConnected(c MQTT.Client) healthcheck.Check {
	return func() error {
		if c.IsConnected() {
			return nil
		}
		return fmt.Errorf("not connected")
	}
}

func getOnMessageReceived(pg chan shared.KafkaMessage) MQTT.MessageHandler {
	return func(client MQTT.Client, msg MQTT.Message) {
		topic := msg.Topic()
		payload := msg.Payload()
		pg <- shared.KafkaMessage{
			Topic: topic,
			Value: payload,
		}
		receivedMessages.Add(1)
	}
}

func onConnectionLost(_ MQTT.Client, _ error) {
	sChan <- true
}

func onConnect(client MQTT.Client) {
	optionsReader := client.OptionsReader()
	zap.S().Infof("Connected to MQTT broker (%s)", optionsReader.ClientID())
}

// newTLSConfig returns the TLS config for a given clientID and mode
func newTLSConfig() *tls.Config {

	// Import trusted certificates from CAfile.pem.
	// Alternatively, manually add CA certificates to
	// default openssl CA bundle.
	certpool := x509.NewCertPool()
	pemCerts, err := os.ReadFile("/SSL_certs/mqtt/ca.crt")
	if err == nil {
		ok := certpool.AppendCertsFromPEM(pemCerts)
		if !ok {
			zap.S().Errorf("Failed to parse root certificate")
		}
	} else {
		zap.S().Errorf("Error reading CA certificate: %s", err)
	}

	zap.S().Debugf("CA cert: %s", pemCerts)

	// Import client certificate/key pair
	cert, err := tls.LoadX509KeyPair("/SSL_certs/mqtt/tls.crt", "/SSL_certs/mqtt/tls.key")
	if err != nil {
		// Read /SSL_certs/mqtt/tls.crt
		var file []byte
		file, err = os.ReadFile("/SSL_certs/mqtt/tls.crt")
		if err != nil {
			zap.S().Errorf("Error reading client certificate: %s", err)
		}
		zap.S().Fatalf("Error reading client certificate: %s (File: %s)", err, file)
	}

	zap.S().Debugf("Client cert: %v", cert)

	// Just to print out the client certificate..
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		zap.S().Fatalf("Error parsing client certificate: %s", err)
	}

	skipVerify, err := env.GetAsBool("INSECURE_SKIP_VERIFY", false, true)
	if err != nil {
		zap.S().Error(err)
	}

	// Create tls.Config with desired tls properties
	/* #nosec G402 -- Remote verification is not yet implemented*/
	return &tls.Config{
		// RootCAs = certs used to verify server cert.
		RootCAs: certpool,
		// ClientAuth = whether to request cert from server.
		// Since the server is set up for SSL, this happens
		// anyways.
		// ClientAuth: tls.NoClientCert,
		// ClientCAs = certs used to validate client cert.
		// ClientCAs: nil,
		// InsecureSkipVerify = verify that cert contents
		// match server. IP matches what is in cert etc.
		/* #nosec G402 -- Remote verification is not yet implemented*/
		InsecureSkipVerify: skipVerify,
		// Certificates = list of certs client sends to server.
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
}

func Shutdown() {
	zap.S().Infof("Shutdown requested")
	mqttClient.Disconnect(250)
	zap.S().Infof("MQTT disconnected")
}

func Start(kafkaToMqttChan chan shared.KafkaMessage) {
	MQTTSenderThreads, err := env.GetAsInt("MQTT_SENDER_THREADS", false, 1)
	if err != nil {
		zap.S().Error(err)
	}
	if MQTTSenderThreads < 1 {
		zap.S().Fatalf("MQTT_SENDER_THREADS must be at least 1")
	}
	for i := 0; i < MQTTSenderThreads; i++ {
		go start(kafkaToMqttChan)
	}
}

func start(kafkaToMqttChan chan shared.KafkaMessage) {
	for {
		msg := <-kafkaToMqttChan
		if !message.IsValidKafkaMessage(msg) {
			continue
		}
		// Change MQTT to Kafka topic format
		msg.Topic = strings.ReplaceAll(msg.Topic, ".", "/")
		mqttClient.Publish(msg.Topic, 1, false, msg.Value)
		sentMessages.Add(1)
	}
}

func GetStats() (s uint64, r uint64) {
	return sentMessages.Load(), receivedMessages.Load()
}
