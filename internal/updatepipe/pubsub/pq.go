package pubsub

import (
	"context"
	"database/sql"
	"time"

	"github.com/foxcpp/maddy/framework/log"
	"github.com/lib/pq"
)

type Msg struct {
	Key     string
	Payload string
}

type PqPubSub struct {
	Notify chan Msg

	L      *pq.Listener
	sender *sql.DB

	Log *log.Logger

	// forwarderDone is closed during Close to ensure the forwarder
	// goroutine exits even when it is blocked sending to Notify and
	// no consumer is reading from it.
	forwarderDone chan struct{}
}

func NewPQ(dsn string) (*PqPubSub, error) {
	l := &PqPubSub{
		Log:           log.DefaultLogger.Sublogger("pgpubsub"),
		Notify:        make(chan Msg),
		forwarderDone: make(chan struct{}),
	}
	l.L = pq.NewListener(dsn, 10*time.Second, time.Minute, l.eventHandler)
	var err error
	l.sender, err = sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(l.Notify)
		for n := range l.L.Notify {
			if n == nil {
				continue
			}

			select {
			case l.Notify <- Msg{Key: n.Channel, Payload: n.Extra}:
			case <-l.forwarderDone:
				return
			}
		}
	}()

	return l, nil
}

func (l *PqPubSub) Close() error {
	if err := l.sender.Close(); err != nil {
		l.Log.Error("failed to close sender socket", err)
	}

	// Close the pq.Listener. This closes the internal l.L.Notify channel,
	// which causes the forwarder's range loop to end once it finishes
	// processing any in-flight message.
	if err := l.L.Close(); err != nil {
		l.Log.Error("failed to close listener", err)
	}

	// Drain any remaining messages from the Notify channel. The forwarder
	// may have sent a message before seeing l.L.Notify close.
	for {
		select {
		case _, ok := <-l.Notify:
			if !ok {
				// Channel closed, forwarder's range loop ended.
				close(l.forwarderDone)
				return nil
			}
			// Drop the message — the consumer (PubSubPipe) has
			// already been signaled to stop.
		case <-l.forwarderDone:
			// Already closed (shouldn't happen, but be safe).
			return nil
		}
	}
}

func (l *PqPubSub) eventHandler(ev pq.ListenerEventType, err error) {
	switch ev {
	case pq.ListenerEventConnected:
		l.Log.DebugMsg("connected")
	case pq.ListenerEventReconnected:
		l.Log.Msg("connection reestablished")
	case pq.ListenerEventConnectionAttemptFailed:
		l.Log.Error("connection attempt failed", err)
	case pq.ListenerEventDisconnected:
		l.Log.Msg("connection closed", "err", err)
	}
}

func (l *PqPubSub) Subscribe(_ context.Context, key string) error {
	return l.L.Listen(key)
}

func (l *PqPubSub) Unsubscribe(_ context.Context, key string) error {
	return l.L.Unlisten(key)
}

func (l *PqPubSub) Publish(key, payload string) error {
	_, err := l.sender.Exec(`SELECT pg_notify($1, $2)`, key, payload)
	return err
}

func (l *PqPubSub) Listener() chan Msg {
	return l.Notify
}
