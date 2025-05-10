package rabbit

import (
	"fmt"

	"analyzer/comms"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderShard struct {
	broker       *Broker
	fmt          string
	key          string
	outputCopies int
	log          *logging.Logger
	seq          []map[int]int
}

func NewShard(broker *Broker, fmt string, key string, outputCopies int, log *logging.Logger) *SenderShard {
	seq := make([]map[int]int, outputCopies)
	for i := range seq {
		seq[i] = make(map[int]int)
	}

	return &SenderShard{
		broker:       broker,
		fmt:          fmt,
		key:          key,
		outputCopies: outputCopies,
		log:          log,
		seq:          seq,
	}
}

func (s *SenderShard) nextKeySeq(i int, clientId int) (string, int) {
	key := fmt.Sprintf(s.fmt, i)
	seq := s.seq[i][clientId]

	s.seq[i][clientId]++
	return key, seq
}

func (s *SenderShard) Batch(batch comms.Batch, filterCols map[string]struct{}, headers amqp.Table) error {
	shards, err := comms.Shard(batch.FieldMaps, s.key, s.outputCopies)
	if err != nil {
		return err
	}
	clientId := int(headers["client-id"].(int32))

	for i, shard := range shards {
		key, seq := s.nextKeySeq(i, clientId)
		body := comms.NewBatch(shard).Encode(filterCols)
		headers["seq"] = seq
		if err := s.broker.Publish(key, body, headers); err != nil {
			s.log.Errorf("error while publishing sharded message to %d: %v", i, err)
			return err
		}
	}

	return nil
}

func (s *SenderShard) Eof(eof comms.Eof, headers amqp.Table) error {
	body := eof.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderShard) Flush(flush comms.Flush, headers amqp.Table) error {
	body := flush.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderShard) Broadcast(body []byte, headers amqp.Table) error {
	clientId := int(headers["client-id"].(int32))

	for i := range s.outputCopies {
		key, seq := s.nextKeySeq(i, clientId)
		headers["seq"] = seq
		if err := s.broker.Publish(key, body, headers); err != nil {
			return err
		}
	}

	return nil
}
