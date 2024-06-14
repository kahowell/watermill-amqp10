package amqp10

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"testing"
)

func TestPubSub(t *testing.T) {
	features := tests.Features{}
	tests.TestPubSub(t, features, constructor, nil)
}

func constructor(t *testing.T) (message.Publisher, message.Subscriber) {
	conn, err := NewConnection(context.Background(), "localhost", nil)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := NewPublisher(context.TODO(), conn)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := NewSubscriber(context.TODO(), conn)
	if err != nil {
		t.Fatal(err)
	}
	return pub, sub
}
