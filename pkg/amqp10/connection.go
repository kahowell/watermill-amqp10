package amqp10

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/go-amqp"
	"github.com/cenkalti/backoff/v4"
	"log/slog"
	"sync"
)

type ConnectionWrapper struct {
	ctx    context.Context
	config Config
	conn   *amqp.Conn
	lock   sync.Mutex
}

func NewConnection(ctx context.Context, config Config) (*ConnectionWrapper, error) {
	connWrapper := &ConnectionWrapper{
		ctx:    ctx,
		config: config,
	}
	if err := connWrapper.connect(); err != nil {
		return nil, err
	}
	return connWrapper, nil
}

func (c *ConnectionWrapper) connect() error {
	protocol := "amqp"
	host := "localhost"
	if c.config.Host != "" {
		host = c.config.Host
	}
	port := 5672
	if c.config.TLSConfig != nil {
		protocol = "amqps"
		port = 5671
	}
	if c.config.Port != 0 {
		port = c.config.Port
	}
	opts := &amqp.ConnOptions{
		TLSConfig: c.config.TLSConfig,
	}
	if c.config.Username != "" && c.config.Password != "" {
		opts.SASLType = amqp.SASLTypePlain(c.config.Username, c.config.Password)
	}
	conn, err := amqp.Dial(c.ctx, fmt.Sprintf("%s://%s:%d", protocol, host, port), opts)
	if err != nil {
		return err
	}
	if conn != nil {
		c.conn = conn
	}
	return nil
}

func (c *ConnectionWrapper) newSession(ctx context.Context) (*amqp.Session, error) {
	c.lock.Lock()
	defer c.lock.Unlock()
	var session *amqp.Session
	err := backoff.Retry(func() error {
		var err error
		session, err = c.conn.NewSession(ctx, nil)
		if err != nil {
			if IsConnErr(err) {
				slog.Warn("Connection error, attempting to reconnect", "error", err)
				reconnErr := c.connect()
				if reconnErr != nil {
					slog.Warn("reconnect failed", "error", err)
				}
				return err
			} else {
				return backoff.Permanent(err)
			}
		}
		return nil
	}, backoff.NewExponentialBackOff())
	if err != nil {
		return nil, err
	}
	return session, nil
}

func IsConnErr(err error) bool {
	connErr := &amqp.ConnError{}
	return errors.As(err, &connErr)
}

func (c *ConnectionWrapper) Close() error {
	return c.conn.Close()
}
