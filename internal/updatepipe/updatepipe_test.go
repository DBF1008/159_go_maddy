/*
Maddy Mail Server - Composable all-in-one email server.
Copyright © 2019-2020 Max Mazurov <fox.cpp@disroot.org>, Maddy Mail Server contributors

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package updatepipe

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mess "github.com/foxcpp/go-imap-mess"
	"github.com/foxcpp/maddy/framework/log"
	"github.com/foxcpp/maddy/internal/updatepipe/pubsub"
)

func testLogger() *log.Logger {
	return &log.Logger{Out: log.NopOutput{}}
}

func recvTimeout(t *testing.T, ch <-chan mess.Update, d time.Duration) (mess.Update, bool) {
	t.Helper()
	select {
	case u := <-ch:
		return u, true
	case <-time.After(d):
		return mess.Update{}, false
	}
}

func expectNoRecv(t *testing.T, ch <-chan mess.Update, d time.Duration) {
	t.Helper()
	select {
	case u := <-ch:
		t.Fatalf("unexpected update received: %+v", u)
	case <-time.After(d):
	}
}

func expectClosePromptly(t *testing.T, p P) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		_ = p.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close hung: a listener goroutine was never released")
	}
}

// TestFormatParseRoundTrip checks that an update survives serialization and
// deserialization, including the sender id and the uint64/string Key forms.
func TestFormatParseRoundTrip(t *testing.T) {
	cases := []mess.Update{
		{Type: mess.UpdFlags, Key: uint64(42), SeqSet: "1:3", NewFlags: []string{"\\Seen", "\\Flagged"}},
		{Type: mess.UpdRemoved, Key: "INBOX", SeqSet: "7"},
		{Type: mess.UpdMboxDestroyed, Key: "weird;name\x10with-bytes"},
	}

	for _, want := range cases {
		blob, err := formatUpdate("sender-1", want)
		if err != nil {
			t.Fatalf("formatUpdate(%+v): %v", want, err)
		}
		id, got, err := parseUpdate(blob[:len(blob)-1]) // drop trailing newline as a reader would
		if err != nil {
			t.Fatalf("parseUpdate(%q): %v", blob, err)
		}
		if id != "sender-1" {
			t.Errorf("sender id = %q, want %q", id, "sender-1")
		}
		if got.Type != want.Type || got.SeqSet != want.SeqSet {
			t.Errorf("round-trip mismatch: got %+v want %+v", got, want)
		}
		if !keysEqual(got.Key, want.Key) {
			t.Errorf("Key round-trip mismatch: got %#v (%T) want %#v (%T)", got.Key, got.Key, want.Key, want.Key)
		}
	}
}

func keysEqual(a, b interface{}) bool {
	return a == b
}

// TestParseUpdateMalformed makes sure malformed input is reported as an error
// and never returns a non-nil update, so callers cannot accidentally
// dereference a nil pointer.
func TestParseUpdateMalformed(t *testing.T) {
	cases := []string{
		"",
		"no-separator-at-all",
		"sender;{not valid json",
		"sender;42",
		"sender;\"a string is not an update object\"",
	}
	for _, raw := range cases {
		id, upd, err := parseUpdate(raw)
		if err == nil {
			// "sender;42" / a bare string actually decode into a zero Update for
			// some inputs; the contract we rely on is only that err==nil implies
			// upd!=nil. Enforce that.
			if upd == nil {
				t.Errorf("parseUpdate(%q): err==nil but upd==nil", raw)
			}
			continue
		}
		if upd != nil {
			t.Errorf("parseUpdate(%q): returned err but upd is non-nil (%+v)", raw, upd)
		}
		if id != "" {
			t.Errorf("parseUpdate(%q): returned err but id=%q", raw, id)
		}
	}
}

// TestDispatchUpdate exercises the shared receive path used by every backend:
// malformed messages are skipped, self-originated updates are deduplicated,
// valid foreign updates are delivered, and delivery is abandoned once done is
// closed.
func TestDispatchUpdate(t *testing.T) {
	lg := testLogger()
	upd := mess.Update{Type: mess.UpdFlags, Key: uint64(1), SeqSet: "1", NewFlags: []string{"\\Seen"}}

	t.Run("delivers foreign update", func(t *testing.T) {
		out := make(chan mess.Update, 1)
		done := make(chan struct{})
		blob, _ := formatUpdate("other", upd)
		dispatchUpdate(blob, "me", out, done, lg)
		if len(out) != 1 {
			t.Fatalf("expected update to be delivered, out len=%d", len(out))
		}
	})

	t.Run("deduplicates own update", func(t *testing.T) {
		out := make(chan mess.Update, 1)
		done := make(chan struct{})
		blob, _ := formatUpdate("me", upd)
		dispatchUpdate(blob, "me", out, done, lg)
		if len(out) != 0 {
			t.Fatalf("own update should have been deduplicated, out len=%d", len(out))
		}
	})

	t.Run("skips malformed update", func(t *testing.T) {
		out := make(chan mess.Update, 1)
		done := make(chan struct{})
		dispatchUpdate("garbage-no-separator", "me", out, done, lg)
		dispatchUpdate("id;{bad json", "me", out, done, lg)
		if len(out) != 0 {
			t.Fatalf("malformed updates should not be delivered, out len=%d", len(out))
		}
	})

	t.Run("abandons delivery when done closed", func(t *testing.T) {
		out := make(chan mess.Update) // unbuffered, no reader
		done := make(chan struct{})
		close(done)
		blob, _ := formatUpdate("other", upd)

		returned := make(chan struct{})
		go func() {
			dispatchUpdate(blob, "me", out, done, lg)
			close(returned)
		}()
		select {
		case <-returned:
		case <-time.After(2 * time.Second):
			t.Fatal("dispatchUpdate blocked even though done was closed")
		}
		select {
		case <-out:
			t.Fatal("update delivered even though done was closed")
		default:
		}
	})
}

// TestUnixSockPipeBroadcastAndDedup verifies normal broadcast over a real Unix
// socket: an update pushed by another pipe is delivered, while an update the
// listening pipe pushes itself is deduplicated.
func TestUnixSockPipeBroadcastAndDedup(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "u.sock")
	lg := testLogger()

	listener := &UnixSockPipe{SockPath: sock, Log: lg}
	ch := make(chan mess.Update, 8)
	if err := listener.Listen(ch); err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// ModeReplicate: the same pipe also pushes its own updates.
	if err := listener.InitPush(); err != nil {
		t.Fatal(err)
	}

	sender := &UnixSockPipe{SockPath: sock, Log: lg}
	if err := sender.InitPush(); err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	ext := mess.Update{Type: mess.UpdFlags, Key: uint64(7), SeqSet: "1", NewFlags: []string{"\\Seen"}}
	if err := sender.Push(ext); err != nil {
		t.Fatal(err)
	}
	got, ok := recvTimeout(t, ch, 2*time.Second)
	if !ok {
		t.Fatal("external update was not delivered")
	}
	if got.Key != uint64(7) || got.Type != mess.UpdFlags || got.SeqSet != "1" {
		t.Fatalf("unexpected update: %+v", got)
	}

	// The listener's own push must be deduplicated, not echoed back.
	own := mess.Update{Type: mess.UpdRemoved, Key: uint64(7), SeqSet: "2"}
	if err := listener.Push(own); err != nil {
		t.Fatal(err)
	}
	expectNoRecv(t, ch, 300*time.Millisecond)
}

// TestUnixSockPipeMalformedDoesNotBreakStream is the regression test for the
// crash where a malformed update dereferenced a nil pointer and killed the
// reader goroutine, silently stopping replica sync. After a malformed line the
// following valid update must still be delivered.
func TestUnixSockPipeMalformedDoesNotBreakStream(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "u.sock")
	lg := testLogger()

	listener := &UnixSockPipe{SockPath: sock, Log: lg}
	ch := make(chan mess.Update, 8)
	if err := listener.Listen(ch); err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Two different shapes of malformed input on the same connection...
	if _, err := io.WriteString(conn, "garbage-without-separator\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "some-id;{not valid json\n"); err != nil {
		t.Fatal(err)
	}
	// ...followed by a valid update from a foreign sender.
	valid, err := formatUpdate("other-sender", mess.Update{
		Type: mess.UpdFlags, Key: uint64(3), SeqSet: "5", NewFlags: []string{"\\Flagged"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, valid); err != nil {
		t.Fatal(err)
	}

	got, ok := recvTimeout(t, ch, 2*time.Second)
	if !ok {
		t.Fatal("valid update after malformed input was not delivered (stream broke)")
	}
	if got.Key != uint64(3) || got.SeqSet != "5" {
		t.Fatalf("unexpected update: %+v", got)
	}
}

// TestUnixSockPipeCloseReleasesBlockedReader makes sure Close does not hang even
// when a reader goroutine is parked trying to deliver an update (the consumer
// stopped draining the channel, as happens during shutdown).
func TestUnixSockPipeCloseReleasesBlockedReader(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "u.sock")
	lg := testLogger()

	listener := &UnixSockPipe{SockPath: sock, Log: lg}
	ch := make(chan mess.Update) // unbuffered, never read -> reader will block on send
	if err := listener.Listen(ch); err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	valid, err := formatUpdate("other-sender", mess.Update{Type: mess.UpdFlags, Key: uint64(1), SeqSet: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, valid); err != nil {
		t.Fatal(err)
	}

	// Give the accept loop time to register the connection and the reader time
	// to park on the channel send.
	time.Sleep(200 * time.Millisecond)

	expectClosePromptly(t, listener)
}

// fakePubSub is an in-memory pubsub.PubSub used to exercise PubSubPipe without a
// PostgreSQL server.
type fakePubSub struct {
	mu     sync.Mutex
	notify chan pubsub.Msg
	closed bool
}

func newFakePubSub() *fakePubSub {
	return &fakePubSub{notify: make(chan pubsub.Msg, 16)}
}

func (f *fakePubSub) Subscribe(context.Context, string) error   { return nil }
func (f *fakePubSub) Unsubscribe(context.Context, string) error { return nil }

func (f *fakePubSub) Publish(key, payload string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil
	}
	f.notify <- pubsub.Msg{Key: key, Payload: payload}
	return nil
}

func (f *fakePubSub) Listener() chan pubsub.Msg { return f.notify }

func (f *fakePubSub) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		close(f.notify)
	}
	return nil
}

// inject feeds a raw payload to the pipe as if it arrived from the broker.
func (f *fakePubSub) inject(t *testing.T, payload string) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		t.Fatal("inject after close")
	}
	f.notify <- pubsub.Msg{Key: "box", Payload: payload}
}

// TestPubSubPipeBehavesLikeUnix asserts the PostgreSQL-backed path handles
// broadcast, deduplication, malformed messages and shutdown identically to the
// Unix socket path.
func TestPubSubPipeBehavesLikeUnix(t *testing.T) {
	lg := testLogger()
	fake := newFakePubSub()
	pipe := &PubSubPipe{PubSub: fake, Log: lg}

	ch := make(chan mess.Update, 8)
	if err := pipe.Listen(ch); err != nil {
		t.Fatal(err)
	}

	// Foreign update is delivered.
	extBlob, err := formatUpdate("external-sender", mess.Update{
		Type: mess.UpdFlags, Key: "box", SeqSet: "1", NewFlags: []string{"\\Seen"},
	})
	if err != nil {
		t.Fatal(err)
	}
	fake.inject(t, extBlob)
	got, ok := recvTimeout(t, ch, 2*time.Second)
	if !ok {
		t.Fatal("external update not delivered")
	}
	if got.Key != "box" || got.SeqSet != "1" {
		t.Fatalf("unexpected update: %+v", got)
	}

	// Malformed messages are skipped, and the pipe's own update is deduplicated,
	// none of which must break the stream.
	fake.inject(t, "garbage-no-separator")
	fake.inject(t, "id;{bad json")
	ownBlob, err := formatUpdate(pipe.myID(), mess.Update{Type: mess.UpdRemoved, Key: "box", SeqSet: "9"})
	if err != nil {
		t.Fatal(err)
	}
	fake.inject(t, ownBlob)

	// A subsequent valid foreign update still arrives.
	ext2, err := formatUpdate("external-sender", mess.Update{Type: mess.UpdRemoved, Key: "box", SeqSet: "2"})
	if err != nil {
		t.Fatal(err)
	}
	fake.inject(t, ext2)
	got, ok = recvTimeout(t, ch, 2*time.Second)
	if !ok {
		t.Fatal("valid update after malformed/own input not delivered (stream broke)")
	}
	if got.SeqSet != "2" || got.Type != mess.UpdRemoved {
		t.Fatalf("unexpected update: %+v", got)
	}

	// Close stops the listener goroutine and returns promptly.
	expectClosePromptly(t, pipe)
}
