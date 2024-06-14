package amqp10

import (
	"context"
	"errors"
	"github.com/Azure/go-amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	"log"
)

type Publisher struct {
	session *amqp.Session
	ctx     context.Context
}

func NewPublisher(ctx context.Context, conn *amqp.Conn) (*Publisher, error) {
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		session: session,
		ctx:     ctx,
	}, nil
}

func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	opts := &amqp.SenderOptions{}
	opts.TargetCapabilities = []string{"queue"}
	sender, err := p.session.NewSender(p.ctx, topic, opts)
	if err != nil {
		return err
	}
	for _, msg := range messages {
		amqpMsg := amqp.NewMessage(msg.Payload)
		amqpMsg.Properties = &amqp.MessageProperties{}
		amqpMsg.Properties.MessageID = msg.UUID
		amqpMsg.Annotations = make(amqp.Annotations)
		for key, value := range msg.Metadata {
			amqpMsg.Annotations[key] = value
		}
		log.Println("Sending message")
		err := sender.Send(p.ctx, amqpMsg, nil)
		log.Println("Message sent")
		if err != nil {
			log.Printf("Error sending message: %s", err)
			return err
		}
	}
	return nil
}

func (p *Publisher) Close() error {
	return p.session.Close(p.ctx)
}

type Subscriber struct {
	ctx         context.Context
	session     *amqp.Session
	cancelFuncs []context.CancelFunc
	conn        *amqp.Conn
}

func NewSubscriber(ctx context.Context, conn *amqp.Conn) (*Subscriber, error) {
	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Subscriber{
		ctx:     ctx,
		conn:    conn,
		session: session,
	}, nil
}

func (s *Subscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	log.Printf("Subscribing to %s", topic)
	session, err := s.conn.NewSession(ctx, nil)
	if err != nil {
		return nil, err
	}
	receiver, err := session.NewReceiver(ctx, topic, nil)
	if err != nil {
		return nil, err
	}

	messageChan := make(chan *message.Message)
	recvCtx, cancel := context.WithCancel(ctx)
	go func() {
		err := receiveMessages(recvCtx, receiver, messageChan)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("Error receiving message: %v", err)
		}
	}()
	s.cancelFuncs = append(s.cancelFuncs, cancel)
	return messageChan, nil
}

func receiveMessages(ctx context.Context, receiver *amqp.Receiver, messageChan chan *message.Message) error {
	log.Println("Start receiving messages")
	for {
		amqpMsg, err := receiver.Receive(ctx, nil)
		if err != nil {
			close(messageChan)
			return err
		}
		log.Printf("Got a message w/ id: %v", amqpMsg.Properties.MessageID)
		msg := message.NewMessage(amqpMsg.Properties.MessageID.(string), amqpMsg.GetData())
		for key, value := range amqpMsg.Annotations {
			msg.Metadata[key.(string)] = value.(string)
		}
		messageChan <- msg
		select {
		case <-msg.Acked():
			err := receiver.AcceptMessage(ctx, amqpMsg)
			if err != nil {
				return err
			}
		case <-msg.Nacked():
			log.Printf("Releasing message with id %v", amqpMsg.Properties.MessageID)
			err = receiver.ReleaseMessage(ctx, amqpMsg)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Subscriber) Close() error {
	for _, cancelFunc := range s.cancelFuncs {
		cancelFunc()
	}
	return s.session.Close(s.ctx)
}

func NewConnection(ctx context.Context, hostname string, opts *amqp.ConnOptions) (*amqp.Conn, error) {
	return amqp.Dial(ctx, hostname, opts)
}
