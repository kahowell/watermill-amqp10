package amqp10

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestRoundTripMarshalUnmarshal(t *testing.T) {
	expected := createTestMessage()
	marshalled, err := Marshal(expected)
	require.NoError(t, err)
	actual, err := Unmarshal(marshalled)

	require.NoError(t, err)
	require.NotNil(t, actual)
	require.True(t, actual.Equals(expected))
}

func createTestMessage() *message.Message {
	msg := message.NewMessage(watermill.NewUUID(), []byte("test"))
	msg.Metadata.Set("foo", "bar")
	return msg
}
