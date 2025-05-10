package impl

import (
	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/groupby/config"

	"github.com/op/go-logging"
)

const SEP = "<|>"

type Groupby struct {
	*workers.Worker
	Con     *config.GroupbyConfig
	Handler GroupbyHandler
}

type GroupbyHandler interface {
	Add(int, map[string]string, *config.GroupbyConfig) error
	Result(int, *config.GroupbyConfig) []map[string]string
	clean(int)
}

func New(con *config.GroupbyConfig, log *logging.Logger) (*Groupby, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	handler := map[string]func(*Groupby) GroupbyHandler{
		"count": NewCount,
		"sum":   NewSum,
		"mean":  NewMean,
	}[con.Aggregator]

	w := &Groupby{base, con, nil}
	w.Handler = handler(w)
	return w, nil
}

func (w *Groupby) Run() error {
	return w.Worker.Run(w)
}

func (w *Groupby) clean(clientId int) {
	w.Handler.clean(clientId)
}

func (w *Groupby) Batch(clientId, qId int, data []byte) {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}

	for _, fieldMap := range batch.FieldMaps {
		err := w.Handler.Add(clientId, fieldMap, w.Con)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}
}

func (w *Groupby) Eof(clientId, qId int, data []byte) {
	responseFieldMaps := w.Handler.Result(clientId, w.Con)
	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	eof := comms.DecodeEof(data)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	w.clean(clientId)
}

func (w *Groupby) Flush(clientId, qId int, data []byte) {
	w.clean(clientId)

	flush := comms.DecodeFlush(data)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
