package impl

import (
	"bytes"
	"encoding/csv"
	"io"
	"strings"

	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/sanitize/config"

	"github.com/op/go-logging"
)

type Sanitize struct {
	*workers.Worker
	Con     *config.SanitizeConfig
	Handler func(*Sanitize, []string) map[string]string
}

func New(con *config.SanitizeConfig, log *logging.Logger) (*Sanitize, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	handler := map[string]func(*Sanitize, []string) map[string]string{
		"movies":  handleMovie,
		"credits": handleCredit,
		"ratings": handleRating,
	}[con.Handler]

	return &Sanitize{base, con, handler}, nil
}

func (w *Sanitize) Run() error {
	return w.Worker.Run(w)
}

func parseNamesFromJson(field string) []string {
	subStr := "'name': '"
	names := make([]string, 0)

	for {
		start := strings.Index(field, subStr)
		if start == -1 {
			break
		}
		start += len(subStr)
		field = field[start:]

		end := strings.Index(field, "'")
		if end == -1 {
			break
		}
		name := field[:end]
		names = append(names, name)

		field = field[end+1:]
	}

	return names
}

func isValidRow(fields map[string]string) bool {
	for _, value := range fields {
		if len(value) == 0 {
			return false
		}
	}
	return true
}

func handleMovie(w *Sanitize, line []string) map[string]string {
	if len(line) != 24 {
		return nil
	}

	for _, i := range []int{2, 3, 5, 9, 13, 14, 15, 17, 20} {
		if len(line[i]) != len(strings.TrimSpace(line[i])) {
			return nil
		}
	}

	// Replace all new lines in overview
	line[9] = strings.ReplaceAll(line[9], "\n", " ")

	genres := parseNamesFromJson(line[3])
	if genres == nil {
		return nil
	}
	prodCountries := parseNamesFromJson(line[13])
	if prodCountries == nil {
		return nil
	}
	spokLangs := parseNamesFromJson(line[17])
	if spokLangs == nil {
		return nil
	}

	fieldMap := map[string]string{
		"id":                   line[5],
		"title":                line[20],
		"release_date":         line[14],
		"overview":             line[9],
		"budget":               line[2],
		"revenue":              line[15],
		"genres":               strings.Join(genres, ","),
		"production_countries": strings.Join(prodCountries, ","),
		"spoken_languages":     strings.Join(spokLangs, ","),
	}

	if !isValidRow(fieldMap) {
		return nil
	}

	return fieldMap
}

func handleRating(w *Sanitize, line []string) map[string]string {
	if len(line) != 4 {
		return nil
	}

	fields := map[string]string{
		"movieId": line[1],
		"rating":  line[2],
	}

	if !isValidRow(fields) {
		return nil
	}

	return fields
}

func handleCredit(w *Sanitize, line []string) map[string]string {
	if len(line) != 3 {
		return nil
	}

	cast := parseNamesFromJson(line[0])
	if cast == nil {
		return nil
	}

	fields := map[string]string{
		"id":   line[2],
		"cast": strings.Join(cast, ","),
	}

	if !isValidRow(fields) {
		return nil
	}

	return fields
}

func (w *Sanitize) Batch(clientId, qId int, data []byte) {
	reader := csv.NewReader(bytes.NewReader(data))
	responseFieldMaps := make([]map[string]string, 0)

	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		}

		if responseFieldMap := w.Handler(w, line); responseFieldMap != nil {
			responseFieldMaps = append(responseFieldMaps, responseFieldMap)
		}
	}

	if len(responseFieldMaps) > 0 {
		w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
		batch := comms.NewBatch(responseFieldMaps)
		if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}
	}
}

func (w *Sanitize) Eof(clientId, qId int, data []byte) {
	eof := comms.DecodeEof(data)
	if err := w.Mailer.PublishEof(eof, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}

func (w *Sanitize) Flush(clientId, qId int, data []byte) {
	body := comms.DecodeFlush(data)
	if err := w.Mailer.PublishFlush(body, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
