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
	"bufio"
	"io"
	"net"
	"os"
	"sync"

	mess "github.com/foxcpp/go-imap-mess"
	"github.com/foxcpp/maddy/framework/log"
	"github.com/foxcpp/maddy/framework/resource/netresource"
)

// UnixSockPipe implements the UpdatePipe interface by serializating updates
// to/from a Unix domain socket. Due to the way Unix sockets work, only one
// Listen goroutine can be running.
//
// The socket is stream-oriented and consists of the following messages:
//
//	SENDER_ID;JSON_SERIALIZED_INTERNAL_OBJECT\n
//
// And SENDER_ID is Process ID and UnixSockPipe address concated as a string.
// It is used to deduplicate updates sent to Push and recevied via Listen.
//
// The SockPath field specifies the socket path to use. The actual socket
// is initialized on the first call to Listen or (Init)Push.
type UnixSockPipe struct {
	SockPath string
	Log      *log.Logger

	listener net.Listener
	sender   net.Conn

	// mu guards the fields below, which are accessed both by the accept/reader
	// goroutines and by Close.
	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	done   chan struct{}

	// wg tracks the accept goroutine and every per-connection reader goroutine
	// so Close can wait for all of them to actually exit.
	wg sync.WaitGroup

	closeOnce sync.Once
}

var _ P = &UnixSockPipe{}

func (usp *UnixSockPipe) myID() string {
	return senderID(usp)
}

func (usp *UnixSockPipe) readUpdates(conn net.Conn, updCh chan<- mess.Update) {
	myID := usp.myID()
	scnr := bufio.NewScanner(conn)
	for scnr.Scan() {
		dispatchUpdate(scnr.Text(), myID, updCh, usp.done, usp.Log)
	}
}

func (usp *UnixSockPipe) Listen(upd chan<- mess.Update) error {
	l, err := netresource.Listen("unix", usp.SockPath)
	if err != nil {
		return err
	}

	usp.mu.Lock()
	usp.listener = l
	usp.done = make(chan struct{})
	usp.conns = make(map[net.Conn]struct{})
	usp.mu.Unlock()

	usp.wg.Add(1)
	go func() {
		defer usp.wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}

			usp.mu.Lock()
			if usp.closed {
				// Close ran between Accept and here; do not start a reader that
				// Close would not be able to account for.
				usp.mu.Unlock()
				conn.Close()
				return
			}
			usp.conns[conn] = struct{}{}
			usp.wg.Add(1)
			usp.mu.Unlock()

			go func(c net.Conn) {
				defer usp.wg.Done()
				usp.readUpdates(c, upd)

				usp.mu.Lock()
				delete(usp.conns, c)
				usp.mu.Unlock()
				c.Close()
			}(conn)
		}
	}()
	return nil
}

func (usp *UnixSockPipe) InitPush() error {
	sock, err := net.Dial("unix", usp.SockPath)
	if err != nil {
		return err
	}

	usp.mu.Lock()
	usp.sender = sock
	usp.mu.Unlock()
	return nil
}

func (usp *UnixSockPipe) Push(upd mess.Update) error {
	usp.mu.Lock()
	sender := usp.sender
	usp.mu.Unlock()
	if sender == nil {
		if err := usp.InitPush(); err != nil {
			return err
		}
		usp.mu.Lock()
		sender = usp.sender
		usp.mu.Unlock()
	}

	updStr, err := formatUpdate(usp.myID(), upd)
	if err != nil {
		return err
	}

	_, err = io.WriteString(sender, updStr)
	return err
}

func (usp *UnixSockPipe) Close() error {
	usp.closeOnce.Do(func() {
		usp.mu.Lock()
		usp.closed = true
		if usp.done != nil {
			close(usp.done)
		}
		conns := make([]net.Conn, 0, len(usp.conns))
		for c := range usp.conns {
			conns = append(conns, c)
		}
		sender := usp.sender
		listener := usp.listener
		usp.mu.Unlock()

		if sender != nil {
			if err := sender.Close(); err != nil {
				usp.Log.Error("failed to close sender socket", err)
			}
		}
		if listener != nil {
			if err := listener.Close(); err != nil {
				usp.Log.Error("failed to close listener", err)
			}
		}
		// Closing the accepted connections unblocks readers that are parked in
		// Scan; the closed done channel unblocks readers parked on the update
		// channel. Together they guarantee every reader returns.
		for _, c := range conns {
			c.Close()
		}

		usp.wg.Wait()

		if listener != nil {
			if err := os.Remove(usp.SockPath); err != nil && !os.IsNotExist(err) {
				usp.Log.Error("failed to remove socket", err)
			}
		}
	})
	return nil
}
