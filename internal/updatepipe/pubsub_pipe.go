package updatepipe

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	mess "github.com/foxcpp/go-imap-mess"
	"github.com/foxcpp/maddy/framework/log"
	"github.com/foxcpp/maddy/internal/updatepipe/pubsub"
)

type PubSubPipe struct {
	PubSub pubsub.PubSub
	Log    *log.Logger

	// done is closed by Close to release the listener goroutine if it is parked
	// trying to deliver an update. wg tracks that goroutine so Close can confirm
	// it exited.
	done chan struct{}
	wg   sync.WaitGroup

	closeOnce sync.Once
}

func (p *PubSubPipe) Listen(upds chan<- mess.Update) error {
	p.done = make(chan struct{})
	myID := p.myID()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for m := range p.PubSub.Listener() {
			dispatchUpdate(m.Payload, myID, upds, p.done, p.Log)
		}
	}()
	return nil
}

func (p *PubSubPipe) InitPush() error {
	return nil
}

func (p *PubSubPipe) myID() string {
	return senderID(p)
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
	var err error
	p.closeOnce.Do(func() {
		// Release a listener goroutine parked on the update channel, then close
		// the underlying PubSub so its Listener channel is closed and the
		// goroutine's range loop terminates.
		if p.done != nil {
			close(p.done)
		}
		err = p.PubSub.Close()
		p.wg.Wait()
	})
	return err
}
