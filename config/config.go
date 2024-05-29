package config

import (
	"crypto/tls"
	"crypto/x509"
	"log"
	"os"
	"strings"
)

// Config stores all configuration of the application
// The values are read by viper from a config file or environment variable
type ConfigVars struct {
	KafkaBootstrapServer []string `mapstructure:"KAFKA_BOOTSTRAP_SERVER"`
	KafkaListenTopic     []string `mapstructure:"KAFKA_LISTEN_TOPIC"`
	KafkaUsername        string   `mapstructure:"KAFKA_USERNAME"`
	KafkaPassword        string   `mapstructure:"KAFKA_PASSWORD"`
	KafkaCertFilePath    string   `mapstructure:"KAFKA_CERT_FILE_PATH"`
	KafkaKeyFilePath     string   `mapstructure:"KAFKA_KEY_FILE_PATH"`
	KafkaClientId        string   `mapstructure:"KAFKA_CLIENT_ID"`
	MQTTCertName         string   `mapstructure:"MQTT_CERTIFICATE_NAME"`
	MQTTUsername         string   `mapstructure:"MQTT_USERNAME"`
	MQTTPassword         string   `mapstructure:"MQTT_PASSWORD"`
	MQTTBrokerURL        string   `mapstructure:"MQTT_BROKER_URL"`
	MQTTTopic            string   `mapstructure:"MQTT_TOPIC"`
	MyPodName            string   `mapstructure:"MY_POD_NAME"`
}

/*
// LoadConfig reads configuration from file or environment variables.

	func LoadConfig() (config ConfigVars, err error) {
		viper.AddConfigPath("./")
		viper.SetConfigName("app")
		viper.SetConfigType("env")

		viper.AutomaticEnv()

		err = viper.ReadInConfig()

		if err != nil {
			log.Println(err)
			return
		}

		err = viper.Unmarshal(&config)
		return
	}
*/
func CreateTlsConfiguration(certFile, keyFile, caFile string, tlsSkipVerify bool) (t *tls.Config) {
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			log.Fatal(err)
		}
		caCertPool := x509.NewCertPool()
		if caFile != "" {
			caCert, err := os.ReadFile(caFile)
			if err != nil {
				log.Println(err)
			}
			caCertPool.AppendCertsFromPEM(caCert)
		}

		t = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: tlsSkipVerify,
		}
	}
	// will be nil by default if nothing is provided
	return t
}
func LoadConfigFromEnvironment() (config ConfigVars, err error) {

	config.KafkaBootstrapServer = strings.Split(os.Getenv("KAFKA_BOOTSTRAP_SERVER"), ",")
	config.KafkaListenTopic = strings.Split(os.Getenv("KAFKA_LISTEN_TOPIC"), ",")
	config.KafkaUsername = os.Getenv("KAFKA_USERNAME")
	config.KafkaPassword = os.Getenv("KAFKA_PASSWORD")
	config.KafkaCertFilePath = os.Getenv("KAFKA_CERT_FILE_PATH")
	config.KafkaKeyFilePath = os.Getenv("KAFKA_KEY_FILE_PATH")
	config.KafkaClientId = os.Getenv("KAFKA_CLIENT_ID")
	config.MQTTCertName = os.Getenv("MQTT_CERTIFICATE_NAME")
	config.MQTTUsername = os.Getenv("MQTT_USERNAME")
	config.MQTTPassword = os.Getenv("MQTT_PASSWORD")
	config.MQTTBrokerURL = os.Getenv("MQTT_BROKER_URL")
	config.MQTTTopic = os.Getenv("MQTT_TOPIC")
	config.MyPodName = os.Getenv("MY_POD_NAME")
	return
}
