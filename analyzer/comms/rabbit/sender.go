package rabbit

import (
	"analyzer/comms"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Sender interface {
	Batch(comms.Batch, map[string]struct{}, amqp.Table) error
	Eof(comms.Eof, amqp.Table) error
	Flush(comms.Flush, amqp.Table) error
}
