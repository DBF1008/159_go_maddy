package updatepipe

import (
	"context"
	"sync"
	"testing"
	"time"

	mess "github.com/foxcpp/go-imap-mess"
	"github.com/foxcpp/maddy/framework/log"
	"github.com/foxcpp/maddy/internal/updatepipe/pubsub"
)

// mockPubSub implements pubsub.PubSub for testing PubSubPipe without a real
// PostgreSQL connection.
type mockPubSub struct {
	notifyCh chan pubsub.Msg

	mu        sync.Mutex
	closed    bool
	closeChan chan struct{} // closed when Close is called
}

func newMockPubSub() *mockPubSub {
	return &mockPubSub{
		notifyCh:  make(chan pubsub.Msg, 32),
		closeChan: make(chan struct{}),
	}
}

func (m *mockPubSub) Subscribe(_ context.Context, _ string) error {
	return nil
}

func (m *mockPubSub) Unsubscribe(_ context.Context, _ string) error {
	return nil
}

func (m *mockPubSub) Publish(_ string, payload string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.notifyCh <- pubsub.Msg{Payload: payload}
	return nil
}

func (m *mockPubSub) Listener() chan pubsub.Msg {
	return m.notifyCh
}

func (m *mockPubSub) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.notifyCh)
		close(m.closeChan)
	}
	return nil
}

var _ pubsub.PubSub = &mockPubSub{}

func newTestPubSubPipe() (*PubSubPipe, *mockPubSub) {
	mock := newMockPubSub()
	pipe := &PubSubPipe{
		PubSub: mock,
		Log:    log.DefaultLogger.Sublogger("test-pubsub"),
	}
	return pipe, mock
}

func TestPubSubPipe_ListenValid(t *testing.T) {
	pipe, mock := newTestPubSubPipe()

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Publish a well-formed update from a different sender ID.
	formatted, err := formatUpdate("other-sender", mess.Update{
		Type:   0,
		Key:    uint64(42),
		SeqSet: "1:5",
	})
	if err != nil {
		t.Fatal(err)
	}
	mock.notifyCh <- pubsub.Msg{Payload: formatted}

	select {
	case upd := <-updCh:
		if upd.Type != 0 {
			t.Errorf("Type: got %d, want 0", upd.Type)
		}
		key, ok := upd.Key.(uint64)
		if !ok {
			t.Fatalf("Key type: got %T, want uint64", upd.Key)
		}
		if key != 42 {
			t.Errorf("Key: got %d, want 42", key)
		}
		if upd.SeqSet != "1:5" {
			t.Errorf("SeqSet: got %q, want %q", upd.SeqSet, "1:5")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update")
	}
}

func TestPubSubPipe_Deduplication(t *testing.T) {
	pipe, mock := newTestPubSubPipe()

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Publish an update with our own sender ID — should be filtered.
	ownFormatted, _ := formatUpdate(pipe.myID(), mess.Update{
		Type: 0,
		Key:  uint64(1),
	})
	mock.notifyCh <- pubsub.Msg{Payload: ownFormatted}

	// Publish an update from a different sender — should be delivered.
	otherFormatted, _ := formatUpdate("other-sender", mess.Update{
		Type: 0,
		Key:  uint64(2),
	})
	mock.notifyCh <- pubsub.Msg{Payload: otherFormatted}

	select {
	case upd := <-updCh:
		key := upd.Key.(uint64)
		if key != 2 {
			t.Errorf("expected update with Key=2 (from other sender), got Key=%d", key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update from other sender")
	}

	// Verify no more updates (our own was filtered).
	select {
	case upd := <-updCh:
		t.Errorf("unexpected extra update: %+v", upd)
	case <-time.After(200 * time.Millisecond):
		// OK — no more updates.
	}
}

func TestPubSubPipe_ListenMalformed(t *testing.T) {
	pipe, mock := newTestPubSubPipe()

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Send a malformed message.
	mock.notifyCh <- pubsub.Msg{Payload: "bad-format-no-semicolon"}

	// Send a well-formed message after the bad one.
	goodFormatted, _ := formatUpdate("other-sender", mess.Update{
		Type: 0,
		Key:  uint64(7),
	})
	mock.notifyCh <- pubsub.Msg{Payload: goodFormatted}

	// The good message should still be delivered.
	select {
	case upd := <-updCh:
		key := upd.Key.(uint64)
		if key != 7 {
			t.Errorf("Key: got %d, want 7", key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for good update after malformed one")
	}
}

func TestPubSubPipe_CloseStopsListen(t *testing.T) {
	pipe, _ := newTestPubSubPipe()

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Close should terminate the Listen goroutine.
	if err := pipe.Close(); err != nil {
		t.Fatal(err)
	}

	// After Close, the mock's notifyCh is closed, so the Listen
	// goroutine's range loop should exit. Give it a moment.
	time.Sleep(100 * time.Millisecond)

	// Verify no more messages can be received.
	select {
	case upd := <-updCh:
		t.Errorf("unexpected update after Close: %+v", upd)
	case <-time.After(200 * time.Millisecond):
		// OK.
	}
}

func TestPubSubPipe_CloseNoDeadlock(t *testing.T) {
	pipe, mock := newTestPubSubPipe()

	// Use a zero-buffer channel: the Listen goroutine will block on send.
	updCh := make(chan mess.Update)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Send a message that the Listen goroutine will try to deliver to
	// updCh. Since nobody reads from updCh, the goroutine blocks.
	formatted, _ := formatUpdate("other-sender", mess.Update{
		Type: 0,
		Key:  uint64(1),
	})
	mock.notifyCh <- pubsub.Msg{Payload: formatted}

	// Give the goroutine time to reach the blocking send.
	time.Sleep(200 * time.Millisecond)

	// Close must not deadlock. It should signal done → goroutine exits.
	done := make(chan struct{})
	go func() {
		pipe.Close()
		close(done)
	}()

	select {
	case <-done:
		// OK — Close completed without deadlocking.
	case <-time.After(3 * time.Second):
		t.Fatal("Close() deadlocked")
	}
}

func TestPubSubPipe_ListenMultipleMessages(t *testing.T) {
	pipe, mock := newTestPubSubPipe()

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Send 5 well-formed messages.
	for i := uint64(0); i < 5; i++ {
		formatted, _ := formatUpdate("other-sender", mess.Update{
			Type: 0,
			Key:  i,
		})
		mock.notifyCh <- pubsub.Msg{Payload: formatted}
	}

	// All 5 should arrive in order.
	for i := uint64(0); i < 5; i++ {
		select {
		case upd := <-updCh:
			key := upd.Key.(uint64)
			if key != i {
				t.Errorf("update %d: Key = %d, want %d", i, key, i)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for update %d", i)
		}
	}
}
