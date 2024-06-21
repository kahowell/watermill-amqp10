package amqp10

import (
	"errors"
	"github.com/Azure/go-amqp"
	"github.com/ThreeDotsLabs/watermill"
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
	var payload []byte
	payload = msg.Payload
	amqpMsg.Value = payload
	return amqpMsg, nil
}

func Unmarshal(amqpMsg *amqp.Message) (*message.Message, error) {
	var watermillUuid string
	if watermillUuidProperty := amqpMsg.ApplicationProperties[watermillUUIDProperty]; watermillUuidProperty != nil {
		watermillUuid = watermillUuidProperty.(string)
	} else {
		watermillUuid = watermill.NewUUID()
	}
	var payload []byte
	if amqpMsg.Value != nil {
		if stringValue, isString := amqpMsg.Value.(string); isString {
			payload = []byte(stringValue)
		} else if binaryValue, isBinary := amqpMsg.Value.([]byte); isBinary {
			payload = binaryValue
		} else {
			return nil, errors.New("unsupported payload type")
		}
	} else {
		payload = amqpMsg.GetData()
	}
	msg := message.NewMessage(watermillUuid, payload)
	for key, value := range amqpMsg.ApplicationProperties {
		if key != watermillUUIDProperty {
			if value == nil {
				msg.Metadata[key] = ""
			} else {
				msg.Metadata[key] = value.(string)
			}
		}
	}
	return msg, nil
}

func GetWatermillUUID(msg *amqp.Message) any {
	return msg.ApplicationProperties[watermillUUIDProperty]
}
