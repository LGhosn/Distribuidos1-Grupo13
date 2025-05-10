package impl

import (
	"fmt"
	"strconv"
	"strings"

	"analyzer/workers/groupby/config"
)

type tuple struct {
	sum float64
	n   int
}

type Mean struct {
	*Groupby
	state map[int]map[string]tuple
}

func NewMean(w *Groupby) GroupbyHandler {
	return &Mean{
		Groupby: w,
		state:   make(map[int]map[string]tuple),
	}
}

func (w *Mean) clean(clientId int) {
	delete(w.state, clientId)
}

func (w *Mean) Add(clientId int, fieldMap map[string]string, con *config.GroupbyConfig) error {
	sumValueStr, ok := fieldMap[con.AggKey]
	if !ok {
		return fmt.Errorf("value %v was not found", con.AggKey)
	}

	sumValue, err := strconv.ParseFloat(sumValueStr, 64)
	if err != nil {
		return fmt.Errorf("the sum value is not numerical %v", sumValueStr)
	}

	keys := make([]string, 0, len(con.GroupKeys))
	for _, key := range con.GroupKeys {
		field, ok := fieldMap[key]
		if !ok {
			return fmt.Errorf("key %v was not found", key)
		}
		keys = append(keys, field)
	}

	compKey := strings.Join(keys, SEP)
	if _, ok := w.state[clientId]; !ok {
		w.state[clientId] = make(map[string]tuple)
	}

	tup := w.state[clientId][compKey]
	w.state[clientId][compKey] = tuple{tup.sum + sumValue, tup.n + 1}
	return nil
}

func (w *Mean) Result(clientId int, con *config.GroupbyConfig) []map[string]string {
	fieldMaps := make([]map[string]string, 0, len(w.state))
	for compKey, v := range w.state[clientId] {
		fieldMap := make(map[string]string)

		keys := strings.Split(compKey, SEP)
		for i, key := range keys {
			fieldMap[con.GroupKeys[i]] = key
		}

		fieldMap[con.Storage] = strconv.FormatFloat(v.sum/float64(v.n), 'f', 4, 64)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	return fieldMaps
}
