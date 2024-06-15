package amqp10

import (
	"context"
	"fmt"
	"github.com/Azure/go-amqp"
)

type ConnectionWrapper struct {
	conn *amqp.Conn
}

func NewConnection(ctx context.Context, config Config) (*ConnectionWrapper, error) {
	protocol := "amqp"
	host := "localhost"
	if config.Host != "" {
		host = config.Host
	}
	port := 5672
	if config.TLSConfig != nil {
		protocol = "amqps"
		port = 5671
	}
	if config.Port != 0 {
		port = config.Port
	}
	opts := &amqp.ConnOptions{
		TLSConfig: config.TLSConfig,
	}
	if config.Username != "" && config.Password != "" {
		opts.SASLType = amqp.SASLTypePlain(config.Username, config.Password)
	}
	conn, err := amqp.Dial(ctx, fmt.Sprintf("%s://%s:%d", protocol, host, port), opts)
	if err != nil {
		return nil, err
	}
	return &ConnectionWrapper{
		conn: conn,
	}, nil
}

func (c *ConnectionWrapper) Close() error {
	return c.conn.Close()
}
