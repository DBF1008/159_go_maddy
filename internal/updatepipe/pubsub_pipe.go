package updatepipe

import (
	"context"
	"fmt"
	"os"
	"strconv"

	mess "github.com/foxcpp/go-imap-mess"
	"github.com/foxcpp/maddy/framework/log"
	"github.com/foxcpp/maddy/internal/updatepipe/pubsub"
)

type PubSubPipe struct {
	PubSub pubsub.PubSub
	Log    *log.Logger

	// done is closed when Close is called to signal the Listen goroutine
	// to exit, preventing deadlocks when the inbound channel is full
	// during shutdown.
	done chan struct{}
}

func (p *PubSubPipe) Listen(upds chan<- mess.Update) error {
	p.done = make(chan struct{})

	go func() {
		for m := range p.PubSub.Listener() {
			id, upd, err := parseUpdate(m.Payload)
			if err != nil {
				p.Log.Error("failed to parse update", err)
				continue
			}
			if id == p.myID() {
				continue
			}
			select {
			case upds <- *upd:
			case <-p.done:
				return
			}
		}
	}()
	return nil
}

func (p *PubSubPipe) InitPush() error {
	return nil
}

func (p *PubSubPipe) myID() string {
	return fmt.Sprintf("%d-%p", os.Getpid(), p)
}

func (p *PubSubPipe) channel(key interface{}) (string, error) {
	var psKey string
	switch k := key.(type) {
	case string:
		psKey = k
	case uint64:
		psKey = "__uint64_" + strconv.FormatUint(k, 10)
	default:
		return "", fmt.Errorf("updatepipe: key type must be either string or uint64")
	}
	return psKey, nil
}

func (p *PubSubPipe) Subscribe(key interface{}) {
	psKey, err := p.channel(key)
	if err != nil {
		p.Log.Error("invalid key passed to Subscribe", err)
		return
	}

	if err := p.PubSub.Subscribe(context.TODO(), psKey); err != nil {
		p.Log.Error("pubsub subscribe failed", err)
	} else {
		p.Log.DebugMsg("subscribed to pubsub", "channel", psKey)
	}
}

func (p *PubSubPipe) Unsubscribe(key interface{}) {
	psKey, err := p.channel(key)
	if err != nil {
		p.Log.Error("invalid key passed to Unsubscribe", err)
		return
	}

	if err := p.PubSub.Unsubscribe(context.TODO(), psKey); err != nil {
		p.Log.Error("pubsub unsubscribe failed", err)
	} else {
		p.Log.DebugMsg("unsubscribed from pubsub", "channel", psKey)
	}
}

func (p *PubSubPipe) Push(upd mess.Update) error {
	psKey, err := p.channel(upd.Key)
	if err != nil {
		return err
	}

	updBlob, err := formatUpdate(p.myID(), upd)
	if err != nil {
		return err
	}

	return p.PubSub.Publish(psKey, updBlob)
}

func (p *PubSubPipe) Close() error {
	// 1. Signal the Listen goroutine to stop sending to the inbound
	//    channel. This unblocks any goroutine waiting in the select
	//    { case upds <- ...; case <-done }.
	if p.done != nil {
		close(p.done)
	}

	// 2. Close the underlying pubsub transport. This closes the
	//    Listener() channel, which causes the range loop in the Listen
	//    goroutine to exit.
	return p.PubSub.Close()
}
