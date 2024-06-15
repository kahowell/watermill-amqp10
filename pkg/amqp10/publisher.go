package amqp10

import (
	"context"
	"github.com/Azure/go-amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"log/slog"
)

type Publisher struct {
	config            PublishConfig
	ctx               context.Context
	conn              *ConnectionWrapper
	cleanupConnection bool
	session           *amqp.Session
}

func NewPublisherWithConnection(ctx context.Context, config Config, conn *ConnectionWrapper) (*Publisher, error) {
	session, err := conn.conn.NewSession(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		config:  config.Publish,
		session: session,
		ctx:     ctx,
		conn:    conn,
	}, nil
}

func NewPublisher(ctx context.Context, config Config) (*Publisher, error) {
	conn, err := NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	pub, err := NewPublisherWithConnection(ctx, config, conn)
	if err != nil {
		return nil, err
	}
	// make pub clean up its connection
	pub.cleanupConnection = true
	return pub, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	durability := amqp.DurabilityNone

	if p.config.Durable {
		durability = amqp.DurabilityUnsettledState
	}
	opts := &amqp.SenderOptions{
		Durability:   durability,
		Capabilities: p.config.Capabilities,
	}
	sender, err := p.session.NewSender(p.ctx, topic, opts)
	if err != nil {
		return err
	}
	for _, msg := range messages {
		amqpMsg, err := Marshal(msg)
		if err != nil {
			return err
		}
		slog.Debug("Sending message", "watermillUUID", msg.UUID)
		err = sender.Send(p.ctx, amqpMsg, nil)
		slog.Debug("Message sent successfully", "watermillUUID", msg.UUID)
		if err != nil {
			slog.Error("Error sending message: %s", err)
			return err
		}
	}
	return nil
}

func (p *Publisher) Close() error {
	err := p.session.Close(p.ctx)
	if err != nil {
		return err
	}
	if p.cleanupConnection {
		return p.conn.Close()
	}
	return nil
}
