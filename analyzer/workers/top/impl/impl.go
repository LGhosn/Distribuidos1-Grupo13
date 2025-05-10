package impl

import (
	"sort"
	"strconv"

	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/top/config"

	"github.com/op/go-logging"
)

type tuple struct {
	value    float64
	fieldMap map[string]string
}

type Top struct {
	*workers.Worker
	Con  *config.TopConfig
	tops map[int][]tuple
}

func New(con *config.TopConfig, log *logging.Logger) (*Top, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	top := Top{
		Worker: base,
		Con:    con,
		tops:   make(map[int][]tuple),
	}

	return &top, nil
}

func (w *Top) Run() error {
	return w.Worker.Run(w)
}

func (w *Top) clean(clientId int) {
	delete(w.tops, clientId)
}

func handleTop(w *Top, clientId int, fieldMap map[string]string) error {
	value, err := strconv.ParseFloat(fieldMap[w.Con.Key], 64)
	if err != nil {
		return err
	}

	top := append(w.tops[clientId], tuple{value, fieldMap})

	sort.Slice(top, func(i, j int) bool {
		return top[i].value > top[j].value
	})

	if len(top) > w.Con.Amount {
		top = top[:w.Con.Amount]
	}

	w.tops[clientId] = top
	return nil
}

func (w *Top) Batch(clientId, qId int, data []byte) {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatal("failed to decode batch: %v", err)
	}

	for _, fieldMap := range batch.FieldMaps {
		err := handleTop(w, clientId, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}
	}
}

func (w *Top) Eof(clientId, qId int, body []byte) {
	responseFieldMaps := make([]map[string]string, 0, w.Con.Amount)
	for _, tup := range w.tops[clientId] {
		responseFieldMaps = append(responseFieldMaps, tup.fieldMap)
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

	eof := comms.DecodeEof(body)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}

	w.clean(clientId)
}

func (w *Top) Flush(clientId, qId int, data []byte) {
	w.clean(clientId)

	body := comms.DecodeFlush(data)
	if err := w.Mailer.PublishFlush(body, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
