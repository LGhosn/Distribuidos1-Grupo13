package impl

import (
	"fmt"
	"strconv"
	"strings"

	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/filter/config"

	"github.com/op/go-logging"
)

type Filter struct {
	*workers.Worker
	Con     *config.FilterConfig
	Handler func(*Filter, map[string]string) (map[string]string, error)
}

func New(con *config.FilterConfig, log *logging.Logger) (*Filter, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	handler := map[string]func(*Filter, map[string]string) (map[string]string, error){
		"range":    handleRange,
		"contains": handleContains,
		"length":   handleLength,
	}[con.Handler]

	return &Filter{base, con, handler}, nil
}

func (w *Filter) Run() error {
	return w.Worker.Run(w)
}

func handleRange(w *Filter, msg map[string]string) (map[string]string, error) {
	yearRange, err := parseMathRange(w.Con.Value)
	if err != nil {
		return nil, err
	}

	date, ok := msg[w.Con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", w.Con.Key)
	}

	year, err := strconv.Atoi(strings.Split(date, "-")[0])
	if err != nil {
		return nil, fmt.Errorf("given year is not a number")
	}

	if !yearRange.Contains(year) {
		return nil, nil
	}

	return msg, nil
}

func handleLength(w *Filter, msg map[string]string) (map[string]string, error) {
	length, err := strconv.Atoi(w.Con.Value)
	if err != nil {
		return nil, fmt.Errorf("given length is not a number")
	}

	values, ok := msg[w.Con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", w.Con.Key)
	}

	if strings.Count(values, ",")+1 != length {
		return nil, nil
	}

	return msg, nil
}

func handleContains(w *Filter, msg map[string]string) (map[string]string, error) {
	values, ok := msg[w.Con.Key]
	if !ok {
		return nil, fmt.Errorf("key %v is not in message", w.Con.Key)
	}

	valueSet := make(map[string]struct{})
	for value := range strings.SplitSeq(values, ",") {
		valueSet[value] = struct{}{}
	}

	for key := range strings.SplitSeq(w.Con.Value, ",") {
		if _, ok := valueSet[key]; !ok {
			return nil, nil
		}
	}

	return msg, nil
}

func (w *Filter) Batch(clientId, qId int, data []byte) {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}
	responseFieldMaps := make([]map[string]string, 0, len(batch.FieldMaps))

	for _, fieldMap := range batch.FieldMaps {
		responseFieldMap, err := w.Handler(w, fieldMap)
		if err != nil {
			w.Log.Errorf("failed to handle message: %v", err)
			continue
		}

		if responseFieldMap != nil {
			responseFieldMaps = append(responseFieldMaps, responseFieldMap)
		}
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		body := comms.NewBatch(responseFieldMaps)
		if err := w.Mailer.PublishBatch(body, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}

}

func (w *Filter) Eof(clientId, qId int, data []byte) {
	eof := comms.DecodeEof(data)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Filter) Flush(clientId, qId int, data []byte) {
	flush := comms.DecodeFlush(data)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
