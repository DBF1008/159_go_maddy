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

// Package smtp_downstream provides target.smtp module that implements
// transparent forwarding or messages to configured list of SMTP servers.
//
// Like remote module, this implementation doesn't handle atomic
// delivery properly since it is impossible to do with SMTP protocol
//
// Interfaces implemented:
// - module.DeliveryTarget
package smtp_downstream

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime/trace"
	"strings"
	"time"

	"github.com/emersion/go-message/textproto"
	"github.com/emersion/go-smtp"
	"github.com/foxcpp/maddy/framework/buffer"
	"github.com/foxcpp/maddy/framework/config"
	tls2 "github.com/foxcpp/maddy/framework/config/tls"
	"github.com/foxcpp/maddy/framework/container"
	"github.com/foxcpp/maddy/framework/exterrors"
	"github.com/foxcpp/maddy/framework/log"
	"github.com/foxcpp/maddy/framework/module"
	"github.com/foxcpp/maddy/framework/module/modules"
	"github.com/foxcpp/maddy/internal/smtpconn"
	"github.com/foxcpp/maddy/internal/smtpconn/pool"
	"github.com/foxcpp/maddy/internal/target"
	"golang.org/x/net/idna"
)

type Downstream struct {
	modName  string
	instName string
	lmtp     bool

	starttls    bool
	hostname    string
	endpoints   []config.Endpoint
	saslFactory saslClientFactory
	tlsConfig   *tls.Config

	connectTimeout    time.Duration
	commandTimeout    time.Duration
	submissionTimeout time.Duration

	// connReuseLimit is the maximum number of SMTP transactions a single
	// pooled connection may serve. When it is 0 connection reuse is disabled
	// and pool is nil, making every delivery open and close its own
	// connection (the historical behavior).
	connReuseLimit int
	pool           *pool.P
	// poolKey is the part of the connection pool key shared by all messages
	// going through this target instance (the configured endpoint set). The
	// authenticated identity is appended per-message in connKey.
	poolKey string

	log *log.Logger
}

func (u *Downstream) moduleError(err error) error {
	if err == nil {
		return nil
	}

	return exterrors.WithFields(err, map[string]interface{}{
		"target": u.modName,
	})
}

func New(c *container.C, modName, instName string) (module.Module, error) {
	return &Downstream{
		modName:  modName,
		instName: instName,
		lmtp:     modName == "target.lmtp",
		log:      c.DefaultLogger.Sublogger(modName),
	}, nil
}

func (u *Downstream) Configure(inlineArgs []string, cfg *config.Map) error {
	var attemptTLS *bool

	targetsArg := make([]string, 0, len(inlineArgs))
	cfg.Bool("debug", true, false, &u.log.Debug)
	cfg.Callback("require_tls", func(m *config.Map, node config.Node) error {
		u.log.Msg("require_tls directive is deprecated and ignored")
		return nil
	})
	cfg.Callback("attempt_starttls", func(m *config.Map, node config.Node) error {
		u.log.Msg("attempt_starttls directive is deprecated and equivalent to starttls")

		if len(node.Args) == 0 {
			trueVal := true
			attemptTLS = &trueVal
			return nil
		}
		if len(node.Args) != 1 {
			return config.NodeErr(node, "expected exactly 1 argument")
		}

		b, err := config.ParseBool(node.Args[0])
		if err != nil {
			return err
		}
		attemptTLS = &b
		return nil
	})
	cfg.Bool("starttls", false, !u.lmtp, &u.starttls)
	cfg.String("hostname", true, true, "", &u.hostname)
	cfg.StringList("targets", false, false, nil, &targetsArg)
	cfg.Custom("auth", false, false, func() (interface{}, error) {
		return nil, nil
	}, saslAuthDirective, &u.saslFactory)
	cfg.Custom("tls_client", true, false, func() (interface{}, error) {
		return &tls.Config{}, nil
	}, tls2.TLSClientBlock, &u.tlsConfig)
	cfg.Duration("connect_timeout", false, false, 5*time.Minute, &u.connectTimeout)
	cfg.Duration("command_timeout", false, false, 5*time.Minute, &u.commandTimeout)
	cfg.Duration("submission_timeout", false, false, 5*time.Minute, &u.submissionTimeout)

	// Connection reuse is opt-in: conn_reuse_limit defaults to 0 which keeps
	// the historical behavior of one connection per delivery.
	cfg.Int("conn_reuse_limit", false, false, 0, &u.connReuseLimit)
	poolCfg := pool.Config{
		MaxKeys:             1000,
		MaxConnsPerKey:      5,      // basically, max. amount of idle connections in cache
		MaxConnLifetimeSec:  150,    // 2.5 mins, half of recommended idle time from RFC 5321
		StaleKeyLifetimeSec: 60 * 4, // make sure that cleanup runs before recommended idle time from RFC 5321
	}
	cfg.Int("conn_max_idle_count", false, false, 5, &poolCfg.MaxConnsPerKey)
	cfg.Int64("conn_max_idle_time", false, false, 150, &poolCfg.MaxConnLifetimeSec)

	if _, err := cfg.Process(); err != nil {
		return err
	}

	if attemptTLS != nil {
		u.starttls = *attemptTLS
	}

	// INTERNATIONALIZATION: See RFC 6531 Section 3.7.1.
	var err error
	u.hostname, err = idna.ToASCII(u.hostname)
	if err != nil {
		return fmt.Errorf("%s: cannot represent the hostname as an A-label name: %w", u.modName, err)
	}

	targetsArg = append(targetsArg, inlineArgs...)
	for _, tgt := range targetsArg {
		endp, err := config.ParseEndpoint(tgt)
		if err != nil {
			return err
		}

		u.endpoints = append(u.endpoints, endp)
	}

	if len(u.endpoints) == 0 {
		return fmt.Errorf("%s: at least one target endpoint is required", u.modName)
	}

	if u.connReuseLimit > 0 {
		// All endpoints of this instance form a single failover set sharing
		// the same TLS and SASL configuration, so connections to any of them
		// are interchangeable and pooled together under one base key.
		addrs := make([]string, 0, len(u.endpoints))
		for _, endp := range u.endpoints {
			addrs = append(addrs, endp.String())
		}
		u.poolKey = strings.Join(addrs, ",")

		// pool.Config.New is left unset on purpose: on a cache miss the pool
		// returns (nil, nil) and we open a connection ourselves so that the
		// per-message metadata (needed for SASL credential forwarding) is
		// threaded through correctly.
		u.pool = pool.New(poolCfg)
	}

	return nil
}

func (u *Downstream) Name() string {
	return u.modName
}

func (u *Downstream) InstanceName() string {
	return u.instName
}

func (u *Downstream) Start() error {
	return nil
}

func (u *Downstream) Stop() error {
	if u.pool != nil {
		u.pool.Close()
	}
	return nil
}

// poolConn wraps smtpconn.C so it can be stored in the connection pool. It
// tracks reuse-related bookkeeping (transaction count, error state) used to
// decide whether the connection is still safe to hand out again.
type poolConn struct {
	*smtpconn.C

	// reuseLimit is the maximum number of transactions this connection may
	// serve (copied from Downstream.connReuseLimit).
	reuseLimit int
	// transactions counts how many SMTP transactions were attempted on this
	// connection so far.
	transactions int
	// errored is set when a transaction left the connection in an
	// indeterminate state (e.g. a failed DATA), making reuse unsafe.
	errored   bool
	lastUseAt time.Time
}

// Usable reports whether the connection can be reused for another transaction.
// It also issues an RSET, both as a liveness probe and to discard any leftover
// transaction state, mirroring the behavior of the remote target.
func (c *poolConn) Usable() bool {
	if c.C == nil || c.transactions > c.reuseLimit || c.Client() == nil || c.errored {
		return false
	}
	return c.C.Client().Reset() == nil
}

func (c *poolConn) LastUseAt() time.Time {
	return c.lastUseAt
}

func (c *poolConn) Close() error {
	return c.C.Close()
}

// connKey computes the connection pool key for a message. Connections
// authenticated as one principal must never be reused for another, so when
// SASL is configured the authenticated identity is mixed into the key. This is
// what makes reuse safe with "auth forward" where credentials vary per message.
func (u *Downstream) connKey(msgMeta *module.MsgMetadata) string {
	if u.saslFactory == nil {
		return u.poolKey
	}
	var authUser string
	if msgMeta.Conn != nil {
		authUser = msgMeta.Conn.AuthUser
	}
	// NUL is not a valid character in either an endpoint address or a
	// username, so it is a safe separator. Only the (non-secret) username is
	// used to avoid leaking credentials through pool key logging.
	return u.poolKey + "\x00" + authUser
}

type delivery struct {
	u   *Downstream
	log *log.Logger

	msgMeta  *module.MsgMetadata
	mailFrom string
	rcpts    []string

	conn *poolConn
	// poolKey is the key under which conn should be returned to the pool. It
	// is set in connect and includes the per-message authenticated identity.
	poolKey string
}

// lmtpDelivery implements module.PartialDelivery
type lmtpDelivery struct {
	*delivery
}

func (u *Downstream) StartDelivery(ctx context.Context, msgMeta *module.MsgMetadata, mailFrom string) (module.Delivery, error) {
	defer trace.StartRegion(ctx, "target.smtp/StartDelivery").End()

	d := &delivery{
		u:        u,
		log:      target.DeliveryLogger(u.log, msgMeta),
		msgMeta:  msgMeta,
		mailFrom: mailFrom,
	}
	if err := d.connect(ctx); err != nil {
		return nil, err
	}

	if err := d.conn.Mail(ctx, mailFrom, msgMeta.SMTPOpts); err != nil {
		if err := d.conn.Close(); err != nil {
			u.log.Error("failed to close smtp connection", err)
		}
		return nil, err
	}

	if u.lmtp {
		return &lmtpDelivery{delivery: d}, nil
	}

	return d, nil
}

func (d *delivery) closeConn(c *smtpconn.C) {
	if err := c.Close(); err != nil {
		d.log.Error("failed to close SMTP connection", err)
	}
}

func (d *delivery) connect(ctx context.Context) error {
	if d.u.pool != nil {
		d.poolKey = d.u.connKey(d.msgMeta)

		pooled, err := d.u.pool.Get(ctx, d.poolKey)
		if err != nil {
			return err
		}
		if pooled != nil {
			d.conn = pooled.(*poolConn)
			d.log.DebugMsg("reusing pooled connection", "downstream_server", d.conn.ServerName(),
				"local_addr", d.conn.LocalAddr(), "remote_addr", d.conn.RemoteAddr(),
				"transactions", d.conn.transactions)
			return nil
		}
	}

	conn, err := d.newConn(ctx)
	if err != nil {
		return err
	}
	d.conn = conn
	return nil
}

// newConn opens a fresh connection to the first reachable endpoint, performing
// SASL authentication if configured. The endpoint failover loop and SASL login
// behavior are identical to the historical (non-pooled) implementation.
func (d *delivery) newConn(ctx context.Context) (*poolConn, error) {
	var lastErr error

	conn := smtpconn.New()
	conn.Log = d.log
	conn.Hostname = d.u.hostname
	conn.AddrInSMTPMsg = false
	if d.u.connectTimeout != 0 {
		conn.ConnectTimeout = d.u.connectTimeout
	}
	if d.u.commandTimeout != 0 {
		conn.CommandTimeout = d.u.commandTimeout
	}
	if d.u.submissionTimeout != 0 {
		conn.SubmissionTimeout = d.u.submissionTimeout
	}

	for _, endp := range d.u.endpoints {
		var err error
		if d.u.lmtp {
			_, err = conn.ConnectLMTP(ctx, endp, d.u.starttls, d.u.tlsConfig)
		} else {
			_, err = conn.Connect(ctx, endp, d.u.starttls, d.u.tlsConfig)
		}
		if err != nil {
			if len(d.u.endpoints) != 1 {
				d.log.Msg("connect error", err, "downstream_server", net.JoinHostPort(endp.Host, endp.Port))
			}
			lastErr = err
			continue
		}

		d.log.DebugMsg("connected", "downstream_server", conn.ServerName())

		lastErr = nil
		break
	}
	if lastErr != nil {
		return nil, d.u.moduleError(lastErr)
	}

	if d.u.saslFactory != nil {
		saslClient, err := d.u.saslFactory(d.msgMeta)
		if err != nil {
			d.closeConn(conn)
			return nil, err
		}

		if err := conn.Client().Auth(saslClient); err != nil {
			d.closeConn(conn)
			return nil, err
		}
	}

	return &poolConn{
		C:          conn,
		reuseLimit: d.u.connReuseLimit,
		lastUseAt:  time.Now(),
	}, nil
}

func (d *delivery) AddRcpt(ctx context.Context, rcptTo string, opts smtp.RcptOptions) error {
	err := d.conn.Rcpt(ctx, rcptTo, opts)
	if err != nil {
		return d.u.moduleError(err)
	}

	d.rcpts = append(d.rcpts, rcptTo)
	return nil
}

func (d *delivery) Body(ctx context.Context, header textproto.Header, body buffer.Buffer) error {
	r, err := body.Open()
	if err != nil {
		return exterrors.WithFields(err, map[string]interface{}{"target": d.u.modName})
	}

	defer func() {
		if err := r.Close(); err != nil {
			d.log.Msg("failed to close body buffer", err)
		}
	}()

	err = d.conn.Data(ctx, header, r)
	if err != nil {
		// A failed DATA may leave the connection in an indeterminate state,
		// so it must not be reused.
		d.conn.errored = true
	}
	return d.u.moduleError(err)
}

func (d *lmtpDelivery) BodyNonAtomic(ctx context.Context, sc module.StatusCollector, header textproto.Header, body buffer.Buffer) {
	r, err := body.Open()
	if err != nil {
		modErr := d.u.moduleError(err)
		for _, rcpt := range d.rcpts {
			sc.SetStatus(rcpt, modErr)
		}
	}
	defer func() {
		if err := r.Close(); err != nil {
			d.log.Msg("failed to close body buffer", err)
		}
	}()

	rcptIndx := 0
	err = d.conn.LMTPData(ctx, header, r, func(rcpt string, err *smtp.SMTPError) {
		if err == nil {
			sc.SetStatus(rcpt, nil)
		} else {
			sc.SetStatus(rcpt, &exterrors.SMTPError{
				Code:         err.Code,
				EnhancedCode: exterrors.EnhancedCode(err.EnhancedCode),
				Message:      err.Message,
				TargetName:   d.u.modName,
				Err:          err,
			})
		}
		rcptIndx++
	})
	if err != nil {
		// An error here (as opposed to a per-recipient status reported via the
		// callback) is a protocol/transport failure that makes the connection
		// unsafe to reuse.
		d.conn.errored = true
		modErr := d.u.moduleError(err)
		for _, rcpt := range d.rcpts[rcptIndx:] {
			sc.SetStatus(rcpt, modErr)
		}
	}
}

func (d *delivery) Abort(ctx context.Context) error {
	return d.finish()
}

func (d *delivery) Commit(ctx context.Context) error {
	return d.finish()
}

// finish releases the connection used by this delivery. With pooling disabled
// the connection is simply closed (the historical behavior). With pooling
// enabled it is returned to the pool when still safe to reuse, or closed
// otherwise (after an error or once the reuse limit is reached).
func (d *delivery) finish() error {
	conn := d.conn
	if conn == nil {
		return nil
	}
	d.conn = nil

	if d.u.pool == nil {
		return conn.Close()
	}

	conn.transactions++
	conn.lastUseAt = time.Now()

	if conn.Usable() {
		d.log.DebugMsg("returning connection to pool", "downstream_server", conn.ServerName(),
			"transactions", conn.transactions)
		d.u.pool.Return(d.poolKey, conn)
		return nil
	}

	d.log.DebugMsg("closing connection", "downstream_server", conn.ServerName(),
		"transactions", conn.transactions, "errored", conn.errored)
	return conn.Close()
}

func init() {
	modules.Register("target.smtp", New)
	modules.Register("target.lmtp", New)
}
