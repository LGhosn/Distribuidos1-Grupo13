package impl

import (
	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/sink/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Sink struct {
	*workers.Worker
	Con *config.SinkConfig
}

func New(con *config.SinkConfig, log *logging.Logger) (*Sink, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}
	return &Sink{base, con}, nil
}

func (w *Sink) Run() error {
	return w.Worker.Run(w)
}

func (w *Sink) Batch(clientId, qId int, data []byte) {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}
	responseFieldMaps := batch.FieldMaps

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		headers := amqp.Table{"query": w.Con.Query}
		if err := w.Mailer.PublishBatch(batch, clientId, headers); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}
}

func (w *Sink) Eof(clientId, qId int, data []byte) {
	eof := comms.DecodeEof(data)
	headers := amqp.Table{"query": w.Con.Query}
	if err := w.Mailer.PublishEof(eof, clientId, headers); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sink) Flush(clientId, qId int, data []byte) {
	body := comms.DecodeFlush(data)
	headers := amqp.Table{"query": w.Con.Query}
	if err := w.Mailer.PublishFlush(body, clientId, headers); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
