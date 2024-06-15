package amqp10

import (
	"crypto/tls"
)

type Config struct {
	Host      string
	Port      int
	Username  string
	Password  string
	TLSConfig *tls.Config
	Publish   PublishConfig
	Subscribe SubscribeConfig
}

type SubscribeConfig struct {
	QueueName    string
	Durable      bool
	Capabilities []string
}

type PublishConfig struct {
	Durable      bool
	Capabilities []string
}
