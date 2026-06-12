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
	"fmt"
	"os"

	mess "github.com/foxcpp/go-imap-mess"
	"github.com/foxcpp/maddy/framework/log"
)

// senderID returns a string that uniquely identifies a pipe object within the
// running process.
//
// Each serialized update is tagged with the senderID of the pipe that pushed
// it. A listening pipe drops updates carrying its own id, which is how an
// update pipe that both sends and receives (ModeReplicate) avoids feeding its
// own updates back into the backend.
func senderID(pipe interface{}) string {
	return fmt.Sprintf("%d-%p", os.Getpid(), pipe)
}

// dispatchUpdate decodes a single serialized update (as produced by
// formatUpdate) received from a transport and delivers it to out.
//
// It is the single receive-path implementation shared by every transport so
// that all backends behave identically regardless of how the bytes arrived:
//
//   - A malformed message is logged and skipped. A single bad update must never
//     interrupt the stream nor crash the reader goroutine; processing of
//     subsequent updates continues unaffected.
//   - An update whose sender id equals myID is dropped: it is an echo of an
//     update this pipe sent itself (see senderID).
//   - Delivery to out is abandoned if done is closed. This keeps a reader from
//     blocking forever on the channel once its consumer has gone away (e.g.
//     during shutdown); the update is dropped in that case. The caller is still
//     responsible for terminating its read loop once the transport is closed.
func dispatchUpdate(raw, myID string, out chan<- mess.Update, done <-chan struct{}, l *log.Logger) {
	id, upd, err := parseUpdate(raw)
	if err != nil {
		l.Error("malformed update received, skipping", err, "raw", raw)
		return
	}

	// It is our own update, skip.
	if id == myID {
		return
	}

	select {
	case out <- *upd:
	case <-done:
	}
}
