package impl

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"maps"
	"os"

	"analyzer/comms"
	"analyzer/workers"
	"analyzer/workers/join/config"

	"github.com/op/go-logging"
)

const (
	STORE = iota
	LOAD
)

const (
	LEFT = iota + 1
	RIGHT
)

const PERSITOR_DIR_PATH = "/persistors"

type PersistMessage struct {
	kind   int
	fields []map[string]string
}

type PersistChannels struct {
	recvs []<-chan []map[string]string
	sends []chan<- PersistMessage
}

type OutOfOrderChannels struct {
	recv <-chan []byte
	send chan<- []map[string]string
}

type Join struct {
	*workers.Worker
	Con          *config.JoinConfig
	readingRight map[int]struct{}
	persistors   map[int]PersistChannels
	outOfOrders  map[int]OutOfOrderChannels
}

func New(con *config.JoinConfig, log *logging.Logger) (*Join, error) {
	base, err := workers.New(con.Config, log)
	if err != nil {
		return nil, err
	}

	w := &Join{
		Worker:       base,
		Con:          con,
		readingRight: make(map[int]struct{}),
		persistors:   make(map[int]PersistChannels),
		outOfOrders:  make(map[int]OutOfOrderChannels),
	}

	return w, nil
}

func (w *Join) setupPersistors(clientId int) {
	recvs := make([]<-chan []map[string]string, 0, w.Con.NShards)
	sends := make([]chan<- PersistMessage, 0, w.Con.NShards)
	for i := range w.Con.NShards {
		recv := make(chan []map[string]string)
		send := make(chan PersistMessage)
		recvs = append(recvs, recv)
		sends = append(sends, send)
		go func(i int) {
			if err := spawnFilePersistor(w, send, recv, clientId, i); err != nil {
				w.Log.Errorf("error in spawnFilePersistor: %v", err)
			}
		}(i)
	}

	w.persistors[clientId] = PersistChannels{recvs, sends}
}

func (w *Join) setupOutOfOrder(clientId int) {
	recv := make(chan []byte)
	send := make(chan []map[string]string)
	go func() {
		if err := spawnOutOfOrderPersistor(w, send, recv, clientId); err != nil {
			w.Log.Errorf("error in spawnOutOfOrderPersistor: %v", err)
		}
	}()

	w.outOfOrders[clientId] = OutOfOrderChannels{recv, send}
}

func (w *Join) clean(clientId int) {
	delete(w.readingRight, clientId)
	for _, send := range w.persistors[clientId].sends {
		close(send)
	}

	delete(w.persistors, clientId)

	if ch, ok := w.outOfOrders[clientId]; ok {
		close(ch.send)
		<-ch.recv
		delete(w.outOfOrders, clientId)
	}

	clientDirPath := fmt.Sprintf("%s/%d", PERSITOR_DIR_PATH, clientId)
	os.RemoveAll(clientDirPath)
}

func store(writer *bufio.Writer, fieldMaps []map[string]string) error {
	data := comms.NewBatch(fieldMaps).EncodeForPersistance()
	writer.Write(data)
	return writer.Flush()
}

func joinFieldMaps(left map[string]string, right map[string]string) map[string]string {
	joined := make(map[string]string, len(left)+len(right))
	maps.Copy(joined, left)
	maps.Copy(joined, right)
	return joined
}

func load(w *Join, fp *os.File, fieldMaps []map[string]string) ([]map[string]string, error) {
	responseFieldMaps := make([]map[string]string, 0)
	fp.Seek(0, 0)
	reader := bufio.NewReader(fp)

	for {
		line, err := reader.ReadBytes('\n')
		line = bytes.TrimSpace(line)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		left, err := comms.DecodeLine(line)
		if err != nil {
			w.Log.Criticalf("failed to decode line: %v", err)
			continue
		}

		valueLeft, ok := left[w.Con.LeftKey]
		if !ok {
			w.Log.Errorf("key %v was not found in left side", w.Con.LeftKey)
			continue
		}

		for _, right := range fieldMaps {
			valueRight, ok := right[w.Con.RightKey]
			if !ok {
				w.Log.Errorf("key %v was not found in right side", w.Con.RightKey)
				continue
			}

			if valueLeft == valueRight {
				joined := joinFieldMaps(left, right)
				responseFieldMaps = append(responseFieldMaps, joined)
			}
		}
	}

	return responseFieldMaps, nil
}

func spawnFilePersistor(w *Join, recv <-chan PersistMessage, send chan<- []map[string]string, clientId, i int) error {
	defer close(send)

	dirPath := fmt.Sprintf("%s/%d", PERSITOR_DIR_PATH, clientId)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("couldn't create directory structure for client %d, peristor %d: %v", clientId, i, err)
	}

	path := fmt.Sprintf("%s/%d.csv", dirPath, i)
	fp, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("the file %v could not be created %v", i, err)
	}
	defer fp.Close()

	writer := bufio.NewWriter(fp)
	for tup := range recv {
		switch tup.kind {
		case STORE:
			if err := store(writer, tup.fields); err != nil {
				w.Log.Errorf("there was an error storing in the file: %v", err)
			}

		case LOAD:
			fieldMaps, err := load(w, fp, tup.fields)
			if err != nil {
				w.Log.Errorf("there was an error loading up field maps: %v", err)
			}

			send <- fieldMaps
		}
	}

	return nil
}

func spawnOutOfOrderPersistor(w *Join, recv <-chan []map[string]string, send chan<- []byte, clientId int) error {
	defer close(send)

	dirPath := fmt.Sprintf("%s/%d", PERSITOR_DIR_PATH, clientId)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("couldn't create directory structure for client %d, out of order peristor: %v", clientId, err)
	}

	path := fmt.Sprintf("%s/out-of-order.csv", dirPath)
	fp, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("the out of order file could not be created %v", err)
	}
	defer fp.Close()

	writer := bufio.NewWriter(fp)
	for fieldMaps := range recv {
		if err := store(writer, fieldMaps); err != nil {
			w.Log.Errorf("there was an error storing in the out of order file: %v", err)
		}
	}

	fp.Seek(0, 0)
	data, err := io.ReadAll(fp)
	if err != nil {
		return fmt.Errorf("there was an error loading the out of order file: %v", err)
	}

	send <- data
	return nil
}

func handleLeft(w *Join, clientId int, data []byte) error {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Criticalf("failed to decode batch: %v", err)
		return err
	}

	shards, err := comms.Shard(batch.FieldMaps, w.Con.LeftKey, w.Con.NShards)
	if err != nil {
		return err
	}

	for i, shard := range shards {
		w.persistors[clientId].sends[i] <- PersistMessage{STORE, shard}
	}

	return nil
}

func handleRight(w *Join, clientId int, data []byte) error {
	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Fatalf("failed to decode batch: %v", err)
	}
	shards, err := comms.Shard(batch.FieldMaps, w.Con.RightKey, w.Con.NShards)
	if err != nil {
		return err
	}

	for i, shard := range shards {
		w.persistors[clientId].sends[i] <- PersistMessage{LOAD, shard}
	}

	for i := range shards {
		responseFieldMaps := <-w.persistors[clientId].recvs[i]
		if len(responseFieldMaps) > 0 {
			w.Log.Debugf("fieldMaps: %v", responseFieldMaps)
			batch := comms.NewBatch(responseFieldMaps)
			if err := w.Mailer.PublishBatch(batch, clientId); err != nil {
				w.Log.Errorf("failed to publish message: %v", err)
			}
		}
	}

	return nil
}

func handleOutOfOrder(w *Join, clientId int, data []byte) error {
	if _, ok := w.outOfOrders[clientId]; !ok {
		w.setupOutOfOrder(clientId)
	}

	batch, err := comms.DecodeBatch(data)
	if err != nil {
		w.Log.Criticalf("failed to decode batch: %v", err)
		return err
	}

	w.outOfOrders[clientId].send <- batch.FieldMaps
	return nil
}

func (w *Join) Run() error {
	return w.Worker.Run(w)
}

func (w *Join) Batch(clientId, qId int, data []byte) {
	if _, ok := w.persistors[clientId]; !ok {
		w.setupPersistors(clientId)
	}

	if _, ok := w.readingRight[clientId]; !ok && qId == LEFT {
		// Reading left and given data is from LEFT queue
		if err := handleLeft(w, clientId, data); err != nil {
			w.Log.Errorf("error while handling batch in left side: %v", err)
		}

	} else if _, ok := w.readingRight[clientId]; ok && qId == RIGHT {
		// Reading right and given data is from RIGHT queue
		if err := handleRight(w, clientId, data); err != nil {
			w.Log.Errorf("error while handling batch in right side: %v", err)
		}

	} else if _, ok := w.readingRight[clientId]; !ok && qId == RIGHT {
		// Reading left and given data is from RIGHT queue
		if err := handleOutOfOrder(w, clientId, data); err != nil {
			w.Log.Errorf("error while handling batch out of order: %v", err)
		}

	} else {
		// Reading right and given data is from LEFT queue
		w.Log.Errorf("unexpected left side message when expecting right for client %d in qId %d", clientId, qId)
	}
}

func (w *Join) Eof(clientId, qId int, data []byte) {
	if _, ok := w.readingRight[clientId]; !ok {
		w.readingRight[clientId] = struct{}{}

		if chans, ok := w.outOfOrders[clientId]; ok {
			close(chans.send)
			data := <-chans.recv

			w.Batch(clientId, RIGHT, data)
			delete(w.outOfOrders, clientId)
		}

	} else {
		eof := comms.DecodeEof(data)
		if err := w.Mailer.PublishEof(eof, clientId); err != nil {
			w.Log.Errorf("failed to publish message: %v", err)
		}

		w.clean(clientId)
	}
}

func (w *Join) Flush(clientId, qId int, data []byte) {
	w.clean(clientId)

	flush := comms.DecodeFlush(data)
	if err := w.Mailer.PublishFlush(flush, clientId); err != nil {
		w.Log.Errorf("failed to publish message: %v", err)
	}
}
