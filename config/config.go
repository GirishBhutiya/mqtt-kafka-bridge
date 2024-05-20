package config

import (
	"log"

	"github.com/spf13/viper"
)

// Config stores all configuration of the application
// The values are read by viper from a config file or environment variable
type ConfigVars struct {
	KafkaBootstrapServer []string `mapstructure:"KAFKA_BOOTSTRAP_SERVER"`
	KafkaListenTopic     []string `mapstructure:"KAFKA_LISTEN_TOPIC"`
	KafkaUsername        string   `mapstructure:"KAFKA_USERNAME"`
	KafkaPassword        string   `mapstructure:"KAFKA_PASSWORD"`
	MQTTCertName         string   `mapstructure:"MQTT_CERTIFICATE_NAME"`
	MQTTUsername         string   `mapstructure:"MQTT_USERNAME"`
	MQTTPassword         string   `mapstructure:"MQTT_PASSWORD"`
	MQTTBrokerURL        string   `mapstructure:"MQTT_BROKER_URL"`
	MQTTTopic            string   `mapstructure:"MQTT_TOPIC"`
	MyPodName            string   `mapstructure:"MY_POD_NAME"`
	ClientId             string   `mapstructure:"CLIENT_ID"`
}

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
