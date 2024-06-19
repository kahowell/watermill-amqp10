package amqp10

import (
	"context"
	"github.com/Azure/go-amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cenkalti/backoff/v4"
	"log/slog"
	"sync"
)

type Publisher struct {
	ctx               context.Context
	lock              sync.Mutex
	config            PublishConfig
	conn              *ConnectionWrapper
	senders           map[string]*amqp.Sender
	cleanupConnection bool
	session           *amqp.Session
}

func NewPublisherWithConnection(ctx context.Context, config Config, conn *ConnectionWrapper) (*Publisher, error) {
	pub := &Publisher{
		config: config.Publish,
		ctx:    ctx,
		conn:   conn,
	}
	if err := pub.connect(); err != nil {
		return nil, err
	}
	return pub, nil
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

func (p *Publisher) connect() error {
	// this drops old sender references
	p.senders = make(map[string]*amqp.Sender)
	session, err := p.conn.newSession(p.ctx)
	if err != nil {
		return err
	}
	p.session = session
	return nil
}

func (p *Publisher) getSender(topic string) (*amqp.Sender, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.senders[topic] != nil {
		return p.senders[topic], nil
	}
	sender, err := p.newSender(topic)
	if err != nil {
		return nil, err
	}
	p.senders[topic] = sender
	return sender, nil
}

func (p *Publisher) newSender(topic string) (*amqp.Sender, error) {
	var sender *amqp.Sender
	err := backoff.Retry(func() error {
		durability := amqp.DurabilityNone
		if p.config.Durable {
			durability = amqp.DurabilityUnsettledState
		}
		opts := &amqp.SenderOptions{
			Durability:   durability,
			Capabilities: p.config.Capabilities,
		}
		var err error
		sender, err = p.session.NewSender(p.ctx, topic, opts)
		if err != nil {
			if IsConnErr(err) {
				slog.Warn("Connection error during sender creation, refreshing session", "error", err)
				refreshErr := p.connect()
				if refreshErr != nil {
					slog.Warn("Error refreshing publisher session", "error", refreshErr)
				}
				return err
			} else {
				return backoff.Permanent(err)
			}
		}
		return nil
	}, &backoff.ZeroBackOff{})
	if err != nil {
		return nil, err
	}
	return sender, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	sender, err := p.getSender(topic)
	if err != nil {
		return err
	}
	for _, msg := range messages {
		amqpMsg, err := Marshal(msg)
		if err != nil {
			return err
		}
		slog.Debug("Sending message", "watermillUUID", msg.UUID)
		err = backoff.Retry(func() error {
			err = sender.Send(p.ctx, amqpMsg, &amqp.SendOptions{Settled: true})
			if err != nil {
				if IsConnErr(err) {
					slog.Warn("Connection error, refreshing senders", "error", err)
					var senderErr error
					p.senders = make(map[string]*amqp.Sender)
					sender, senderErr = p.getSender(topic)
					if senderErr != nil {
						slog.Warn("Error refreshing sender", "error", senderErr)
					}
					return err
				} else {
					return backoff.Permanent(err)
				}
			}
			return nil
		}, &backoff.ZeroBackOff{})
		if err != nil {
			slog.Error("Error sending message: %s", err)
			return err
		}
		slog.Debug("Message sent successfully", "watermillUUID", msg.UUID)
	}
	return nil
}

func (p *Publisher) Close() error {
	p.CloseSenders()
	if err := p.session.Close(p.ctx); err != nil {
		if IsConnErr(err) {
			slog.Warn("Error closing publisher session", "error", err)
		} else {
			return err
		}
	}
	if p.cleanupConnection {
		return p.conn.Close()
	}
	return nil
}

func (p *Publisher) CloseSenders() {
	for _, sender := range p.senders {
		if err := sender.Close(p.ctx); err != nil {
			slog.Warn("Error closing sender", "error", err)
		}
	}
	p.senders = make(map[string]*amqp.Sender)
}
