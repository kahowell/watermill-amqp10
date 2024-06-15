package amqp10

import (
	"context"
	"errors"
	"github.com/Azure/go-amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"log/slog"
)

type Subscriber struct {
	ctx               context.Context
	config            SubscribeConfig
	cleanupConnection bool
	conn              *ConnectionWrapper
	session           *amqp.Session
	cancelFuncs       []context.CancelFunc
	closedChan        chan struct{}
}

func NewSubscriber(ctx context.Context, config Config) (*Subscriber, error) {
	conn, err := NewConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	sub, err := NewSubscriberWithConnection(ctx, config, conn)
	if err != nil {
		return nil, err
	}
	sub.cleanupConnection = true
	return sub, nil
}

func NewSubscriberWithConnection(ctx context.Context, config Config, conn *ConnectionWrapper) (*Subscriber, error) {
	session, err := conn.conn.NewSession(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Subscriber{
		config:     config.Subscribe,
		ctx:        ctx,
		conn:       conn,
		session:    session,
		closedChan: make(chan struct{}),
	}, nil
}

func (s *Subscriber) SubscribeInitialize(topic string) error {
	session, err := s.conn.conn.NewSession(s.ctx, nil)
	if err != nil {
		return err
	}
	receiver, err := newReceiver(s, topic, session)
	if err != nil {
		return err
	}
	err = receiver.Close(s.ctx)
	if err != nil {
		return err
	}
	return session.Close(s.ctx)
}

func newReceiver(s *Subscriber, topic string, session *amqp.Session) (*amqp.Receiver, error) {
	durability := amqp.DurabilityNone
	if s.config.Durable {
		durability = amqp.DurabilityUnsettledState
	}
	opts := &amqp.ReceiverOptions{
		SourceCapabilities: s.config.Capabilities,
		Durability:         durability,
	}
	return session.NewReceiver(s.ctx, topic, opts)
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	slog.Debug("Subscribing", "topic", topic)
	receiver, err := newReceiver(s, topic, s.session)
	if err != nil {
		return nil, err
	}

	messageChan := make(chan *message.Message)
	recvCtx, cancel := context.WithCancel(ctx)
	go func() {
		err := s.receiveMessages(recvCtx, receiver, messageChan)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Error receiving message", "error", err)
		}
		err = receiver.Close(s.ctx)
		if err != nil {
			slog.Error("Error closing receiver", "error", err)
		}
	}()
	s.cancelFuncs = append(s.cancelFuncs, cancel)
	return messageChan, nil
}

func (s *Subscriber) receiveMessages(ctx context.Context, receiver *amqp.Receiver, messageChan chan *message.Message) error {
	slog.Debug("Start receiving messages")
	for {
		err := s.processMessage(ctx, receiver, messageChan)
		if err != nil {
			close(messageChan)
			return err
		}
	}
}

func (s *Subscriber) processMessage(ctx context.Context, receiver *amqp.Receiver, messageChan chan *message.Message) error {
	amqpMsg, err := receiver.Receive(ctx, nil)
	if err != nil {
		return err
	}
	slog.Debug("Received message", "watermillUUID", GetWatermillUUID(amqpMsg))
	msg, err := Unmarshal(amqpMsg)
	ctx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	msg.SetContext(ctx)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		slog.Debug("Processing context done before message emitted", "watermillUUID", GetWatermillUUID(amqpMsg))
		return nackMsg(ctx, receiver, amqpMsg)
	case messageChan <- msg:
		slog.Debug("Message sent to channel", "watermillUUID", msg.UUID)
	case <-s.closedChan:
		slog.Debug("Channel closed before delivery attempted", "watermillUUID", msg.UUID)
		return nackMsg(ctx, receiver, amqpMsg)
	}
	select {
	case <-s.closedChan:
		slog.Debug("Channel being closed, nacking message")
		return nackMsg(ctx, receiver, amqpMsg)
	case <-msg.Acked():
		return ackMsg(ctx, receiver, amqpMsg)
	case <-msg.Nacked():
		return nackMsg(ctx, receiver, amqpMsg)
	}
}

func (s *Subscriber) Close() error {
	for _, cancelFunc := range s.cancelFuncs {
		cancelFunc()
	}
	err := s.session.Close(s.ctx)
	if err != nil {
		return err
	}
	if s.conn != nil {
		return s.conn.Close()
	}
	close(s.closedChan)
	return nil
}

func ackMsg(ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message) error {
	slog.Debug("Acking message", "watermillUUID", GetWatermillUUID(msg))
	err := receiver.AcceptMessage(ctx, msg)
	if err != nil {
		return err
	}
	return nil
}

func nackMsg(ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message) error {
	slog.Debug("Releasing message", "watermillUUID", GetWatermillUUID(msg))
	err := receiver.ReleaseMessage(ctx, msg)
	if err != nil {
		slog.Debug("Error during nack", "error", err)
		return err
	}
	return nil
}
