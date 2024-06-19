package amqp10

import (
	"context"
	"errors"
	"github.com/Azure/go-amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/cenkalti/backoff/v4"
	"log/slog"
	"sync"
)

type Subscriber struct {
	ctx               context.Context
	lock              sync.Mutex
	config            SubscribeConfig
	cleanupConnection bool
	conn              *ConnectionWrapper
	session           *amqp.Session
	cancelFuncs       []context.CancelFunc
	closedChan        chan struct{}
	closed            bool
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
	s := &Subscriber{
		config:     config.Subscribe,
		ctx:        ctx,
		conn:       conn,
		closedChan: make(chan struct{}),
	}
	if err := s.connect(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Subscriber) SubscribeInitialize(topic string) error {
	session, err := s.conn.newSession(s.ctx)
	if err != nil {
		return err
	}
	receiver, err := s.newReceiver(topic)
	if err != nil {
		return err
	}
	err = receiver.Close(s.ctx)
	if err != nil {
		return err
	}
	return session.Close(s.ctx)
}

func (s *Subscriber) connect() error {
	session, err := s.conn.newSession(s.ctx)
	if err != nil {
		return err
	}
	s.session = session
	return nil
}

func (s *Subscriber) newReceiver(topic string) (*amqp.Receiver, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	durability := amqp.DurabilityNone
	if s.config.Durable {
		durability = amqp.DurabilityUnsettledState
	}
	opts := &amqp.ReceiverOptions{
		SourceCapabilities: s.config.Capabilities,
		Durability:         durability,
	}
	var receiver *amqp.Receiver
	err := backoff.Retry(func() error {
		var err error
		receiver, err = s.session.NewReceiver(s.ctx, topic, opts)
		if err != nil {
			if IsConnErr(err) {
				slog.Warn("Connection error during receiver creation, refreshing session", "error", err)
				reconnErr := s.connect()
				if reconnErr != nil {
					slog.Warn("Error refreshing subscriber session", "error", reconnErr)
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
	return receiver, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	slog.Debug("Subscribing", "topic", topic)
	receiver, err := s.newReceiver(topic)
	if err != nil {
		return nil, err
	}

	messageChan := make(chan *message.Message)
	recvCtx, cancel := context.WithCancel(ctx)
	go func() {
		err := backoff.Retry(func() error {
			err := s.receiveMessages(recvCtx, receiver, messageChan)
			if err != nil {
				if IsConnErr(err) {
					slog.Warn("Connection error receiving messages, refreshing receiver", "error", err)
					var recvErr error
					receiver, recvErr = s.newReceiver(topic)
					if recvErr != nil {
						slog.Warn("Error creating new receiver", "error", recvErr)
					}
					return err
				} else {
					close(messageChan)
					if errors.Is(err, context.Canceled) {
						err := receiver.Close(s.ctx)
						if err != nil {
							slog.Warn("Error closing receiver", "error", err)
						}
					} else {
						slog.Error("Error receiving message", "error", err)
					}
					return backoff.Permanent(err)
				}
			}
			err = receiver.Close(s.ctx)
			if err != nil {
				slog.Error("Error closing receiver", "error", err)
			}
			return nil
		}, &backoff.ZeroBackOff{})
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("Error receiving messages", "error", err)
		}
	}()
	s.cancelFuncs = append(s.cancelFuncs, cancel)
	return messageChan, nil
}

func (s *Subscriber) receiveMessages(ctx context.Context, receiver *amqp.Receiver, messageChan chan *message.Message) error {
	slog.Debug("Start receiving messages")
	for {
		if err := s.processMessage(ctx, receiver, messageChan); err != nil {
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
		return nackMsg(context.TODO(), receiver, amqpMsg)
	case messageChan <- msg:
		slog.Debug("Message sent to channel", "watermillUUID", msg.UUID)
	case <-s.closedChan:
		slog.Debug("Channel closed before delivery attempted", "watermillUUID", msg.UUID)
		return nackMsg(context.TODO(), receiver, amqpMsg)
	}
	select {
	case <-s.closedChan:
		slog.Debug("Channel being closed, nacking message")
		return nackMsg(context.TODO(), receiver, amqpMsg)
	case <-msg.Acked():
		return ackMsg(context.TODO(), receiver, amqpMsg)
	case <-msg.Nacked():
		return nackMsg(context.TODO(), receiver, amqpMsg)
	}
}

func (s *Subscriber) Close() error {
	for _, cancelFunc := range s.cancelFuncs {
		cancelFunc()
	}
	if err := s.session.Close(s.ctx); err != nil {
		if IsConnErr(err) {
			slog.Warn("Error closing subscriber session", "error", err)
		} else {
			return err
		}
	}
	if s.conn != nil && s.cleanupConnection {
		return s.conn.Close()
	}
	if !s.closed {
		close(s.closedChan)
		s.closed = true
	}
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
