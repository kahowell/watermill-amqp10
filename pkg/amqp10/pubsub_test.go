package amqp10

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/ThreeDotsLabs/watermill/pubsub/tests"
	"github.com/stretchr/testify/require"
	"log/slog"
	"os"
	"testing"
)

func TestPubSub(t *testing.T) {
	slogOpts := &slog.HandlerOptions{}
	slogOpts.Level = slog.LevelDebug
	handler := slog.NewTextHandler(os.Stdout, slogOpts)
	slog.SetDefault(slog.New(handler))
	tests.TestPubSub(t, tests.Features{
		Persistent: true,
	}, constructor, nil)
}

func constructor(t *testing.T) (message.Publisher, message.Subscriber) {
	publishConfig := PublishConfig{
		Capabilities: []string{"queue"},
	}
	subscribeConfig := SubscribeConfig{
		Capabilities: []string{"queue"},
	}
	config := Config{
		Publish:   publishConfig,
		Subscribe: subscribeConfig,
	}
	ctx := context.TODO()
	conn, err := NewConnection(ctx, config)
	require.NoError(t, err)
	pub, err := NewPublisherWithConnection(ctx, config, conn)
	require.NoError(t, err)
	sub, err := NewSubscriberWithConnection(ctx, config, conn)
	require.NoError(t, err)
	return pub, sub
}
