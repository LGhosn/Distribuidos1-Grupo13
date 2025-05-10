package workers

import (
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"syscall"

	"analyzer/comms"
	"analyzer/workers/config"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type IWorker interface {
	Batch(int, int, []byte)
	Eof(int, int, []byte)
	Flush(int, int, []byte)
}

type Worker struct {
	Log       *logging.Logger
	Mailer    *Mailer
	recvCases []reflect.SelectCase
	con       *config.Config
}

func New(con *config.Config, log *logging.Logger) (*Worker, error) {
	mailer, err := NewMailer(con, log)
	if err != nil {
		return nil, err
	}

	inputQueues, err := mailer.Init()
	if err != nil {
		return nil, err
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sigs)},
	}

	for i, q := range inputQueues {
		ch, err := mailer.Consume(q)
		if err != nil {
			return nil, fmt.Errorf("couldn't start consuming: error with queue %d: %v", i, err)
		}

		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		})
	}

	return &Worker{
		Log:       log,
		Mailer:    mailer,
		recvCases: cases,
		con:       con,
	}, nil
}

func (base *Worker) Run(w IWorker) error {
	base.Log.Infof("Running...")
	cases := base.recvCases

	for {
		qId, value, ok := reflect.Select(cases)
		if !ok {
			return fmt.Errorf("ok in reflective select is false, channel got closed unexpectedly for qId %d", qId)
		}

		if qId == 0 {
			base.Log.Info("received SIGTERM")
			return nil
		}

		del := value.Interface().(amqp.Delivery)
		kind := int(del.Headers["kind"].(int32))
		client := int(del.Headers["client-id"].(int32))
		body := del.Body

		switch kind {
		case comms.BATCH:
			w.Batch(client, qId, body)
		case comms.EOF:
			w.Eof(client, qId, body)
		case comms.FLUSH:
			base.Log.Infof("received flush message for client %d", client)
			w.Flush(client, qId, body)
		default:
			base.Log.Errorf("received an unknown message type %v", kind)
		}

		if err := del.Ack(false); err != nil {
			base.Log.Errorf("error while acknowledging message: %v", err)
		}
	}
}

func (w *Worker) Close() {
	w.Mailer.DeInit()
}
