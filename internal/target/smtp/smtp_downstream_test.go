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
	"errors"
	"flag"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/emersion/go-smtp"
	"github.com/foxcpp/maddy/framework/config"
	"github.com/foxcpp/maddy/framework/exterrors"
	"github.com/foxcpp/maddy/framework/module"
	"github.com/foxcpp/maddy/internal/smtpconn/pool"
	"github.com/foxcpp/maddy/internal/testutils"
	"github.com/stretchr/testify/require"
)

var testPort string

func TestDownstreamDelivery(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	tarpit := testutils.FailOnConn(t, "127.0.0.2:"+testPort)
	defer func() {
		require.NoError(t, tarpit.Close())
	}()

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
			{
				Scheme: "tcp",
				Host:   "127.0.0.2",
				Port:   testPort,
			},
		},
		log: testutils.Logger(t, "target.smtp"),
	}

	testutils.DoTestDelivery(t, mod, "test@example.invalid", []string{"rcpt@example.invalid"})
	be.CheckMsg(t, 0, "test@example.invalid", []string{"rcpt@example.invalid"})
}

func TestDownstreamDelivery_LMTP(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort, func(srv *smtp.Server) {
		srv.LMTP = true
	})
	be.LMTPDataErr = []error{
		nil,
		&smtp.SMTPError{
			Code:    501,
			Message: "nop",
		},
	}
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		modName: "target.lmtp",
		lmtp:    true,
		log:     testutils.Logger(t, "lmtp_downstream"),
	}

	sc := make(statusCollector)

	testutils.DoTestDeliveryNonAtomic(t, &sc, mod, "test@example.invalid", []string{"rcpt1@example.invalid", "rcpt2@example.invalid"})
	be.CheckMsg(t, 0, "test@example.invalid", []string{"rcpt1@example.invalid", "rcpt2@example.invalid"})

	if len(sc) != 2 {
		t.Fatal("Two statuses should be set")
	}
	if err := sc["rcpt1@example.invalid"]; err != nil {
		t.Fatal("Unexpected error for rcpt1:", err)
	}
	if sc["rcpt2@example.invalid"] == nil {
		t.Fatal("Expected an error for rcpt2")
	}
	var rcptErr *exterrors.SMTPError
	if !errors.As(sc["rcpt2@example.invalid"], &rcptErr) {
		t.Fatalf("Not SMTPError: %T", rcptErr)
	}
	if rcptErr.Code != 501 {
		t.Fatal("Wrong SMTP code:", rcptErr.Code)
	}
}

func TestDownstreamDelivery_LMTP_ErrorCoerce(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort, func(srv *smtp.Server) {
		srv.LMTP = true
	})
	be.LMTPDataErr = []error{
		nil,
		&smtp.SMTPError{
			Code:    501,
			Message: "nop",
		},
	}
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		modName: "target.lmtp",
		lmtp:    true,
		log:     testutils.Logger(t, "lmtp_downstream"),
	}

	_, err := testutils.DoTestDeliveryErr(t, mod, "test@example.invalid", []string{"rcpt1@example.invalid", "rcpt2@example.invalid"})
	if err == nil {
		t.Error("expected failure")
	}
}

type statusCollector map[string]error

func (sc *statusCollector) SetStatus(rcptTo string, err error) {
	(*sc)[rcptTo] = err
}

func TestDownstreamDelivery_Fallback(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.2:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
			{
				Scheme: "tcp",
				Host:   "127.0.0.2",
				Port:   testPort,
			},
		},
		log: testutils.Logger(t, "target.smtp"),
	}

	testutils.DoTestDelivery(t, mod, "test@example.invalid", []string{"rcpt@example.invalid"})
	be.CheckMsg(t, 0, "test@example.invalid", []string{"rcpt@example.invalid"})
}

func TestDownstreamDelivery_MAILErr(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	be.MailErr = &smtp.SMTPError{
		Code:         550,
		EnhancedCode: smtp.EnhancedCode{5, 1, 2},
		Message:      "Hey",
	}

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		log: testutils.Logger(t, "target.smtp"),
	}

	_, err := testutils.DoTestDeliveryErr(t, mod, "test@example.invalid", []string{"rcpt@example.invalid"})
	testutils.CheckSMTPErr(t, err, 550, exterrors.EnhancedCode{5, 1, 2}, "Hey")
}

func TestDownstreamDelivery_StartTLS(t *testing.T) {
	clientCfg, be, srv := testutils.SMTPServerSTARTTLS(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		tlsConfig: clientCfg.Clone(),
		starttls:  true,
		log:       testutils.Logger(t, "target.smtp"),
	}

	testutils.DoTestDelivery(t, mod, "test@example.invalid", []string{"rcpt@example.invalid"})
	be.CheckMsg(t, 0, "test@example.invalid", []string{"rcpt@example.invalid"})

	tlsState, ok := be.Messages[0].Conn.TLSConnectionState()
	if !ok || !tlsState.HandshakeComplete {
		t.Fatal("Message was not delivered over TLS")
	}
}

func TestDownstreamDelivery_StartTLS_NoFallback(t *testing.T) {
	_, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		starttls: true,
		log:      testutils.Logger(t, "target.smtp"),
	}

	_, err := testutils.DoTestDeliveryErr(t, mod, "test@example.invalid", []string{"rcpt@example.invalid"})
	if err == nil {
		t.Error("Expected an error, got none")
	}
}

// poolTestConfig is a permissive pool configuration used by the connection
// reuse tests: it keeps idle connections cached effectively forever so that
// reuse decisions are driven solely by the reuse limit and connection error
// state, not by background expiry.
func poolTestConfig() pool.Config {
	return pool.Config{
		MaxKeys:             100,
		MaxConnsPerKey:      5,
		MaxConnLifetimeSec:  9999,
		StaleKeyLifetimeSec: 9999,
	}
}

func TestDownstreamDelivery_ConnReuse(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		connReuseLimit: 10,
		poolKey:        "127.0.0.1:" + testPort,
		pool:           pool.New(poolTestConfig()),
		log:            testutils.Logger(t, "target.smtp"),
	}
	// Closing the pool drops the idle connection it holds; do it before the
	// deferred leak check runs (defers execute LIFO).
	defer mod.Stop()

	testutils.DoTestDelivery(t, mod, "test1@example.invalid", []string{"rcpt1@example.invalid"})
	testutils.DoTestDelivery(t, mod, "test2@example.invalid", []string{"rcpt2@example.invalid"})

	be.CheckMsg(t, 0, "test1@example.invalid", []string{"rcpt1@example.invalid"})
	be.CheckMsg(t, 1, "test2@example.invalid", []string{"rcpt2@example.invalid"})

	// Both messages must have travelled over a single backend session.
	if be.SessionCounter != 1 {
		t.Errorf("expected a single reused connection, got %d sessions", be.SessionCounter)
	}
}

func TestDownstreamDelivery_ConnReuse_Limit(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		connReuseLimit: 1,
		poolKey:        "127.0.0.1:" + testPort,
		pool:           pool.New(poolTestConfig()),
		log:            testutils.Logger(t, "target.smtp"),
	}
	defer mod.Stop()

	// With conn_reuse_limit=1 a connection may serve two transactions before it
	// is retired (reuse is denied once the transaction count exceeds the
	// limit), so four deliveries require exactly two connections.
	for i := 0; i < 4; i++ {
		testutils.DoTestDelivery(t, mod, "test@example.invalid", []string{"rcpt@example.invalid"})
	}

	if be.SessionCounter != 2 {
		t.Errorf("expected reuse limit to force 2 connections, got %d sessions", be.SessionCounter)
	}
}

func TestDownstreamDelivery_ConnReuse_ErroredNotReused(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		connReuseLimit: 10,
		poolKey:        "127.0.0.1:" + testPort,
		pool:           pool.New(poolTestConfig()),
		log:            testutils.Logger(t, "target.smtp"),
	}
	defer mod.Stop()

	// A failed DATA leaves the connection in an indeterminate state, so it must
	// be dropped instead of returned to the pool.
	be.DataErr = &smtp.SMTPError{
		Code:    450,
		Message: "temporary failure",
	}
	if _, err := testutils.DoTestDeliveryErr(t, mod, "test1@example.invalid", []string{"rcpt1@example.invalid"}); err == nil {
		t.Fatal("expected delivery to fail")
	}

	be.DataErr = nil
	testutils.DoTestDelivery(t, mod, "test2@example.invalid", []string{"rcpt2@example.invalid"})
	be.CheckMsg(t, 0, "test2@example.invalid", []string{"rcpt2@example.invalid"})

	// The second delivery must have opened a fresh connection rather than
	// reusing the errored one.
	if be.SessionCounter != 2 {
		t.Errorf("expected errored connection to be dropped (2 sessions), got %d", be.SessionCounter)
	}
}

func TestDownstreamDelivery_ConnReuse_AuthIsolation(t *testing.T) {
	be, srv := testutils.SMTPServer(t, "127.0.0.1:"+testPort)
	defer func() {
		require.NoError(t, srv.Close())
	}()
	defer testutils.CheckSMTPConnLeak(t, srv)

	mod := &Downstream{
		hostname: "mx.example.invalid",
		endpoints: []config.Endpoint{
			{
				Scheme: "tcp",
				Host:   "127.0.0.1",
				Port:   testPort,
			},
		},
		saslFactory:    testSaslFactory(t, "forward"),
		connReuseLimit: 10,
		poolKey:        "127.0.0.1:" + testPort,
		pool:           pool.New(poolTestConfig()),
		log:            testutils.Logger(t, "target.smtp"),
	}
	defer mod.Stop()

	deliverAs := func(from, rcpt, user, pass string) {
		t.Helper()
		testutils.DoTestDeliveryMeta(t, mod, from, []string{rcpt}, &module.MsgMetadata{
			Conn: &module.ConnState{
				AuthUser:     user,
				AuthPassword: pass,
			},
		})
	}

	// Two deliveries authenticated as the same user may share a connection.
	deliverAs("a1@example.invalid", "r1@example.invalid", "alice", "pass")
	deliverAs("a2@example.invalid", "r2@example.invalid", "alice", "pass")
	// A delivery authenticated as a different user must NOT reuse alice's
	// connection: connKey mixes the authenticated identity into the pool key so
	// credentials are never shared across principals.
	deliverAs("b1@example.invalid", "r3@example.invalid", "bob", "pass")

	if be.SessionCounter != 2 {
		t.Errorf("expected 2 connections (alice reused, bob separate), got %d sessions", be.SessionCounter)
	}
	if be.Messages[0].AuthUser != "alice" || be.Messages[1].AuthUser != "alice" {
		t.Errorf("expected first two messages authenticated as alice, got %q and %q",
			be.Messages[0].AuthUser, be.Messages[1].AuthUser)
	}
	if be.Messages[2].AuthUser != "bob" {
		t.Errorf("expected third message authenticated as bob, got %q", be.Messages[2].AuthUser)
	}
}

func TestMain(m *testing.M) {
	remoteSmtpPort := flag.String("test.smtpport", "random", "(maddy) SMTP port to use for connections in tests")
	flag.Parse()

	if *remoteSmtpPort == "random" {
		*remoteSmtpPort = strconv.Itoa(rand.Intn(65536-10000) + 10000)
	}

	testPort = *remoteSmtpPort
	os.Exit(m.Run())
}
