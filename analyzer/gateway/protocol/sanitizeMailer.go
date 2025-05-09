package protocol

import (
	"analyzer/comms"
	"analyzer/comms/rabbit"
	"analyzer/gateway/config"
	"fmt"
	"reflect"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SanitizeMailer struct {
	broker      *rabbit.Broker
	con         *config.Config
	senders     []*rabbit.SenderRobin
	receivers   []*rabbit.Receiver
	filename2Id map[string]int
	outputQfmts []string
	log         *logging.Logger
}

func NewSanitizeMailer(con *config.Config, log *logging.Logger) (*SanitizeMailer, error) {
	broker, err := rabbit.NewBroker(con.Url)
	if err != nil {
		return nil, err
	}

	return &SanitizeMailer{
		broker:  broker,
		senders: nil,
		con:     con,
		log:     log,
		filename2Id: map[string]int{
			"movies":  0,
			"credits": 1,
			"ratings": 2,
		},
	}, nil
}

func (s *SanitizeMailer) initSenders(outputQFmts []string) []*rabbit.SenderRobin {
	outputQCopies := s.con.OutputCopies
	senders := make([]*rabbit.SenderRobin, 0, len(outputQCopies))

	for i := range outputQFmts {
		sender := rabbit.NewRobin(s.broker, outputQFmts[i], outputQCopies[i])
		senders = append(senders, sender)
	}

	return senders
}

func (s *SanitizeMailer) initReceivers(inputQs []amqp.Queue, inputCopies []int) []*rabbit.Receiver {
	receivers := make([]*rabbit.Receiver, 0, len(inputQs))

	for i := range inputQs {
		recv := rabbit.NewReceiver(s.broker, inputQs[i], inputCopies[i])
		receivers = append(receivers, recv)
	}

	return receivers
}

func (s *SanitizeMailer) Init() error {
	inExchNames := s.con.InputExchangeNames
	inQNames := s.con.InputQueueNames
	outExchName := s.con.OutputExchangeName
	outQNames := s.con.OutputQueueNames
	outCopies := s.con.OutputCopies

	inputQs, outputQfmts, err := s.broker.Init(s.con.Id, inExchNames, inQNames, outExchName, outQNames, outCopies)
	if err != nil {
		return err
	}

	inputCopies := s.con.InputCopies
	s.outputQfmts = outputQfmts
	s.senders = s.initSenders(outputQfmts)
	s.receivers = s.initReceivers(inputQs, inputCopies)

	return nil
}

func (s *SanitizeMailer) NewSendingMailer() (*SanitizeMailer, error) {
	mailer, err := NewSanitizeMailer(s.con, s.log)
	if err != nil {
		return nil, fmt.Errorf("couldn't create sending mailer: %v", err)
	}

	mailer.outputQfmts = s.outputQfmts
	mailer.senders = mailer.initSenders(mailer.outputQfmts)
	return mailer, err
}

func (s *SanitizeMailer) Consume() (<-chan amqp.Delivery, error) {
	out := make(chan amqp.Delivery)

	cases := make([]reflect.SelectCase, len(s.receivers))
	for i, recv := range s.receivers {
		ch, err := recv.Consume("")
		if err != nil {
			return nil, fmt.Errorf("couldn't start consuming: error with chan %d: %v", i, err)
		}

		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	go func() {
		defer close(out)

		for len(cases) > 0 {
			chosen, recv, ok := reflect.Select(cases)
			if !ok {
				cases[chosen] = cases[len(cases)-1]
				cases = cases[:len(cases)-1]
				continue
			}

			out <- recv.Interface().(amqp.Delivery)
		}
	}()

	return out, nil
}

func (s *SanitizeMailer) PublishBatch(fileName string, clientId int, body []byte) error {
	baseHeaders := amqp.Table{
		"kind":       comms.BATCH,
		"replica-id": s.con.Id,
		"client-id":  int32(clientId),
	}
	return s.senders[s.filename2Id[fileName]].Direct(body, baseHeaders)
}

func (s *SanitizeMailer) PublishEof(fileName string, clientId int, body []byte) error {
	baseHeaders := amqp.Table{
		"kind":       comms.EOF,
		"replica-id": s.con.Id,
		"client-id":  int32(clientId),
	}
	return s.senders[s.filename2Id[fileName]].Broadcast(body, baseHeaders)
}

func (s *SanitizeMailer) DeInit() {
	s.broker.DeInit()
}
