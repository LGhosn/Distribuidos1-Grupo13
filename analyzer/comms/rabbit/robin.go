package rabbit

import (
	"fmt"

	"analyzer/comms"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SenderRobin struct {
	broker       *Broker
	fmt          string
	cur          map[int]int
	outputCopies int
	seq          []map[int]int
}

func NewRobin(broker *Broker, fmt string, outputCopies int) *SenderRobin {
	seq := make([]map[int]int, outputCopies)
	for i := range seq {
		seq[i] = make(map[int]int)
	}

	return &SenderRobin{
		broker:       broker,
		fmt:          fmt,
		outputCopies: outputCopies,
		cur:          make(map[int]int),
		seq:          seq,
	}
}

func (s *SenderRobin) Batch(batch comms.Batch, filterCols map[string]struct{}, headers amqp.Table) error {
	body := batch.Encode(filterCols)
	return s.Direct(body, headers)
}

func (s *SenderRobin) Eof(eof comms.Eof, headers amqp.Table) error {
	body := eof.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderRobin) Flush(flush comms.Flush, headers amqp.Table) error {
	body := flush.Encode()
	return s.Broadcast(body, headers)
}

func (s *SenderRobin) nextKeySeq(clientId int) (string, int) {
	i := s.cur[clientId]
	key := fmt.Sprintf(s.fmt, i)
	seq := s.seq[i][clientId]

	s.seq[i][clientId]++
	s.cur[clientId] = (i + 1) % s.outputCopies
	return key, seq
}

func (s *SenderRobin) Direct(body []byte, headers amqp.Table) error {
	clientId := int(headers["client-id"].(int32))
	key, seq := s.nextKeySeq(clientId)
	headers["seq"] = seq
	return s.broker.Publish(key, body, headers)
}

func (s *SenderRobin) Broadcast(body []byte, headers amqp.Table) error {
	for range s.outputCopies {
		if err := s.Direct(body, headers); err != nil {
			return err
		}
	}
	return nil
}
