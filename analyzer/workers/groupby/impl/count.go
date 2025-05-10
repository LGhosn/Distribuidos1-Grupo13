package impl

import (
	"fmt"
	"strconv"
	"strings"

	"analyzer/workers/groupby/config"
)

type Count struct {
	*Groupby
	state map[int]map[string]int
}

func NewCount(w *Groupby) GroupbyHandler {
	return &Count{
		Groupby: w,
		state:   make(map[int]map[string]int),
	}
}

func (w *Count) clean(clientId int) {
	delete(w.state, clientId)
}

func (w *Count) Add(clientId int, fieldMap map[string]string, con *config.GroupbyConfig) error {
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
		w.state[clientId] = make(map[string]int)
	}

	w.state[clientId][compKey] += 1
	return nil
}

func (w *Count) Result(clientId int, con *config.GroupbyConfig) []map[string]string {
	fieldMaps := make([]map[string]string, 0, len(w.state))
	for compKey, v := range w.state[clientId] {
		fieldMap := make(map[string]string)

		keys := strings.Split(compKey, SEP)
		for i, key := range keys {
			fieldMap[con.GroupKeys[i]] = key
		}

		fieldMap[con.Storage] = strconv.Itoa(v)
		fieldMaps = append(fieldMaps, fieldMap)
	}

	return fieldMaps
}
