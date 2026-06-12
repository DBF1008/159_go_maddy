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

package smtp_downstream

import (
	"time"

	"github.com/foxcpp/maddy/internal/smtpconn"
)

// downstreamConn wraps smtpconn.C to implement pool.Conn for downstream
// connection reuse. It tracks the number of SMTP transactions performed on
// the connection and whether any error occurred that makes the connection
// unsafe to reuse.
type downstreamConn struct {
	*smtpconn.C

	reuseLimit   int
	transactions int
	errored      bool
	lastUse      time.Time
}

// Usable reports whether the connection can be safely returned to the pool for
// reuse. It checks that the connection has not exceeded its reuse limit, has
// not encountered errors, and responds to RSET (which verifies liveness).
func (c *downstreamConn) Usable() bool {
	if c.C == nil || c.transactions > c.reuseLimit || c.Client() == nil || c.errored {
		return false
	}
	return c.C.Client().Reset() == nil
}

// LastUseAt returns the timestamp of the last SMTP transaction on this
// connection. Used by the pool to evict stale connections.
func (c *downstreamConn) LastUseAt() time.Time {
	return c.lastUse
}

// Close sends QUIT and closes the underlying SMTP connection.
func (c *downstreamConn) Close() error {
	return c.C.Close()
}
