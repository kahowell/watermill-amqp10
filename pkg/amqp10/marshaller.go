package amqp10

import (
	"github.com/Azure/go-amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

const watermillUUIDProperty = "_watermill_message_uuid"

func Marshal(msg *message.Message) (*amqp.Message, error) {
	amqpMsg := amqp.NewMessage(msg.Payload)
	amqpMsg.Header = &amqp.MessageHeader{
		Durable: true,
	}
	amqpMsg.ApplicationProperties = make(map[string]interface{})
	for key, value := range msg.Metadata {
		amqpMsg.ApplicationProperties[key] = value
	}
	amqpMsg.ApplicationProperties[watermillUUIDProperty] = msg.UUID
	return amqpMsg, nil
}

func Unmarshal(amqpMsg *amqp.Message) (*message.Message, error) {
	watermillUuid := amqpMsg.ApplicationProperties[watermillUUIDProperty].(string)
	msg := message.NewMessage(watermillUuid, amqpMsg.GetData())
	for key, value := range amqpMsg.ApplicationProperties {
		if key != watermillUUIDProperty {
			msg.Metadata[key] = value.(string)
		}
	}
	return msg, nil
}

func GetWatermillUUID(msg *amqp.Message) any {
	return msg.ApplicationProperties[watermillUUIDProperty]
}
