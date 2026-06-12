package updatepipe

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	mess "github.com/foxcpp/go-imap-mess"
	"github.com/foxcpp/maddy/framework/log"
)

// newTestUnixPipe creates a UnixSockPipe with a unique temporary socket path.
// The caller must call Close when done.
func newTestUnixPipe(t *testing.T) *UnixSockPipe {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "test.sock")
	return &UnixSockPipe{
		SockPath: sockPath,
		Log:      log.DefaultLogger.Sublogger("test-unixpipe"),
	}
}

func TestUnixSockPipe_PushListen(t *testing.T) {
	pipe := newTestUnixPipe(t)

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()

	// Connect a separate sender (simulating another process).
	sender := &UnixSockPipe{
		SockPath: pipe.SockPath,
		Log:      log.DefaultLogger.Sublogger("test-sender"),
	}
	if err := sender.InitPush(); err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	want := mess.Update{
		Type:   0,
		Key:    uint64(42),
		SeqSet: "1:5",
	}
	if err := sender.Push(want); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-updCh:
		if got.Type != want.Type {
			t.Errorf("Type: got %d, want %d", got.Type, want.Type)
		}
		gotKey, ok := got.Key.(uint64)
		if !ok {
			t.Fatalf("Key type: got %T, want uint64", got.Key)
		}
		if gotKey != 42 {
			t.Errorf("Key: got %d, want 42", gotKey)
		}
		if got.SeqSet != want.SeqSet {
			t.Errorf("SeqSet: got %q, want %q", got.SeqSet, want.SeqSet)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for update")
	}
}

func TestUnixSockPipe_Deduplication(t *testing.T) {
	pipe := newTestUnixPipe(t)

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()

	// Use the same pipe to push (same myID → updates should be filtered).
	if err := pipe.InitPush(); err != nil {
		t.Fatal(err)
	}

	if err := pipe.Push(mess.Update{Type: 0, Key: uint64(1)}); err != nil {
		t.Fatal(err)
	}

	select {
	case upd := <-updCh:
		t.Errorf("own update should have been filtered, got: %+v", upd)
	case <-time.After(500 * time.Millisecond):
		// OK — own update was correctly deduplicated.
	}
}

func TestUnixSockPipe_MalformedMessage(t *testing.T) {
	pipe := newTestUnixPipe(t)

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()

	// Connect a raw writer to send both malformed and well-formed data.
	conn, err := net.Dial("unix", pipe.SockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Send malformed data (no semicolon delimiter).
	conn.Write([]byte("garbage-data-no-semicolon\n"))

	// Send a well-formed update from a different sender.
	goodMsg, _ := formatUpdate("different-sender", mess.Update{
		Type:   0,
		Key:    uint64(7),
		SeqSet: "3",
	})
	conn.Write([]byte(goodMsg))

	// The good message should arrive despite the preceding bad one.
	select {
	case upd := <-updCh:
		key, ok := upd.Key.(uint64)
		if !ok {
			t.Fatalf("Key type: got %T, want uint64", upd.Key)
		}
		if key != 7 {
			t.Errorf("Key: got %d, want 7", key)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out: good message not received after malformed one")
	}
}

func TestUnixSockPipe_CloseStopsGoroutines(t *testing.T) {
	pipe := newTestUnixPipe(t)

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Establish a connection so a reader goroutine exists.
	conn, err := net.Dial("unix", pipe.SockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Give the accept goroutine time to spawn readUpdates.
	time.Sleep(100 * time.Millisecond)

	// Close should terminate all goroutines.
	done := make(chan struct{})
	go func() {
		pipe.Close()
		close(done)
	}()

	select {
	case <-done:
		// OK — Close completed.
	case <-time.After(3 * time.Second):
		t.Fatal("Close() did not complete in time — possible goroutine leak")
	}
}

func TestUnixSockPipe_CloseNoDeadlock(t *testing.T) {
	pipe := newTestUnixPipe(t)

	// Use a zero-buffer channel: the reader goroutine will block on
	// send when it receives a message.
	updCh := make(chan mess.Update)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Send a message so the reader goroutine blocks on the send to
	// the full (unbuffered) channel.
	conn, err := net.Dial("unix", pipe.SockPath)
	if err != nil {
		t.Fatal(err)
	}

	msg, _ := formatUpdate("other-sender", mess.Update{
		Type: 0,
		Key:  uint64(1),
	})
	conn.Write([]byte(msg))

	// Give the reader goroutine time to reach the blocking send.
	time.Sleep(200 * time.Millisecond)

	// Close must not deadlock. It should close done → goroutine exits
	// via the select case, then close the connection → Scan returns.
	closeDone := make(chan struct{})
	go func() {
		pipe.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// OK — Close completed without deadlocking.
	case <-time.After(3 * time.Second):
		t.Fatal("Close() deadlocked")
	}

	conn.Close()
}

func TestUnixSockPipe_MultipleConnections(t *testing.T) {
	pipe := newTestUnixPipe(t)

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}
	defer pipe.Close()

	// Connect two separate senders.
	conn1, err := net.Dial("unix", pipe.SockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn1.Close()

	conn2, err := net.Dial("unix", pipe.SockPath)
	if err != nil {
		t.Fatal(err)
	}
	defer conn2.Close()

	// Give accept goroutine time to handle both connections.
	time.Sleep(100 * time.Millisecond)

	// Send from each connection.
	msg1, _ := formatUpdate("sender-1", mess.Update{Type: 0, Key: uint64(10)})
	conn1.Write([]byte(msg1))

	msg2, _ := formatUpdate("sender-2", mess.Update{Type: 1, Key: uint64(20)})
	conn2.Write([]byte(msg2))

	received := make(map[uint64]bool)
	for i := 0; i < 2; i++ {
		select {
		case upd := <-updCh:
			key := upd.Key.(uint64)
			received[key] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for update %d", i+1)
		}
	}

	if !received[10] || !received[20] {
		t.Errorf("expected updates with keys 10 and 20, got %v", received)
	}
}

func TestUnixSockPipe_CloseRemovesSocket(t *testing.T) {
	pipe := newTestUnixPipe(t)

	updCh := make(chan mess.Update, 10)
	if err := pipe.Listen(updCh); err != nil {
		t.Fatal(err)
	}

	// Socket file should exist after Listen.
	if _, err := os.Stat(pipe.SockPath); err != nil {
		t.Fatalf("socket file missing after Listen: %v", err)
	}

	if err := pipe.Close(); err != nil {
		t.Fatal(err)
	}

	// Socket file should be removed after Close.
	if _, err := os.Stat(pipe.SockPath); !os.IsNotExist(err) {
		t.Errorf("socket file still exists after Close (err=%v)", err)
	}
}
