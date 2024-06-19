# Watermill AMQP 1.0 Pub/Sub

This is an unofficial Watermill Pub/Sub implementation for AMQP 1.0. Note the AMQP 1.0 protocol itself supports a
multitude of broker configurations, and this implementation focuses on durable messages, using durable queues on
ActiveMQ Artemis (PRs welcome to support/test more use cases).

## Contributing

PRs welcome!

`podman-compose up` can be used to start an ActiveMQ Artemis message broker. The Watermill Universal Pub/Sub test suite
can be run via `go test` (see [pubsub_test.go](pkg/amqp10/pubsub_test.go))

## License

[MIT License](./LICENSE)
