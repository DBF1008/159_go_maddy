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
	saslCfg     *saslConfig
	tlsConfig   *tls.Config

	connectTimeout    time.Duration
	commandTimeout    time.Duration
	submissionTimeout time.Duration

	// Connection reuse (pooling) configuration.
	// When connReuseLimit is 0, pooling is disabled (nil pool).
	connReuseLimit   int
	connMaxIdleCount int
	connMaxIdleTime  int64
	pool             *pool.P

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
	}, saslAuthDirective, &u.saslCfg)
	cfg.Custom("tls_client", true, false, func() (interface{}, error) {
		return &tls.Config{}, nil
	}, tls2.TLSClientBlock, &u.tlsConfig)
	cfg.Duration("connect_timeout", false, false, 5*time.Minute, &u.connectTimeout)
	cfg.Duration("command_timeout", false, false, 5*time.Minute, &u.commandTimeout)
	cfg.Duration("submission_timeout", false, false, 5*time.Minute, &u.submissionTimeout)
	cfg.Int("conn_reuse_limit", false, false, 0, &u.connReuseLimit)
	cfg.Int("conn_max_idle_count", false, false, 5, &u.connMaxIdleCount)
	cfg.Int64("conn_max_idle_time", false, false, 150, &u.connMaxIdleTime)

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
		u.pool = pool.New(pool.Config{
			MaxKeys:             5000,
			MaxConnsPerKey:      u.connMaxIdleCount,
			MaxConnLifetimeSec:  u.connMaxIdleTime,
			StaleKeyLifetimeSec: u.connMaxIdleTime * 2,
		})
	}

	return nil
}

func (u *Downstream) Name() string {
	return u.modName
}

func (u *Downstream) InstanceName() string {
	return u.instName
}

// Start implements container.LifetimeModule. It is a no-op for downstream
// targets — the pool (if any) is created during Configure.
func (u *Downstream) Start() error {
	return nil
}

// Stop implements container.LifetimeModule. It drains and closes all idle
// connections in the pool.
func (u *Downstream) Stop() error {
	if u.pool != nil {
		u.pool.Close()
	}
	return nil
}

type delivery struct {
	u   *Downstream
	log *log.Logger

	msgMeta  *module.MsgMetadata
	mailFrom string
	rcpts    []string

	conn       *smtpconn.C
	pooledConn *downstreamConn // non-nil when pooling is active
	poolKey    string          // pool key for Return; empty when pooling disabled
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
		d.closeConn(d.conn)
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
	var lastErr error

	for _, endp := range d.u.endpoints {
		// --- Try pool first (if enabled) ---
		if d.u.pool != nil {
			pk := d.u.poolKeyFor(endp, d.msgMeta)
			pooledConn, _ := d.u.pool.Get(ctx, pk)
			if pooledConn != nil {
				dc := pooledConn.(*downstreamConn)
				dc.Log = d.log
				d.conn = dc.C
				d.pooledConn = dc
				d.poolKey = pk
				d.log.DebugMsg("reusing pooled connection", "downstream_server", dc.ServerName(),
					"transactions", dc.transactions)
				return nil
			}
		}

		// --- Create a fresh connection ---
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

		// SASL authentication (new connections only; pooled connections are
		// already authenticated for the credentials encoded in the pool key).
		if d.u.saslCfg != nil && d.u.saslCfg.factory != nil {
			saslClient, saslErr := d.u.saslCfg.factory(d.msgMeta)
			if saslErr != nil {
				d.closeConn(conn)
				lastErr = saslErr
				continue
			}

			if authErr := conn.Client().Auth(saslClient); authErr != nil {
				d.closeConn(conn)
				lastErr = authErr
				continue
			}
		}

		d.conn = conn

		// Wrap in downstreamConn for pool tracking if pooling is enabled.
		if d.u.pool != nil {
			pk := d.u.poolKeyFor(endp, d.msgMeta)
			d.pooledConn = &downstreamConn{
				C:          conn,
				reuseLimit: d.u.connReuseLimit,
				lastUse:    time.Now(),
			}
			d.poolKey = pk
		}

		return nil
	}

	if lastErr != nil {
		return d.u.moduleError(lastErr)
	}
	return d.u.moduleError(fmt.Errorf("no endpoints configured"))
}

// poolKeyFor computes the pool key for the given endpoint and message
// metadata. The key encodes both the endpoint identity (scheme + address) and
// the SASL credentials so that connections authenticated with different
// credentials are never shared.
func (u *Downstream) poolKeyFor(endp config.Endpoint, msgMeta *module.MsgMetadata) string {
	key := endp.Scheme + "://" + endp.Address()
	if u.saslCfg != nil && u.saslCfg.identity != nil {
		key += "\x00" + u.saslCfg.identity(msgMeta)
	}
	return key
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
	dataErr := d.conn.Data(ctx, header, r)
	if dataErr != nil && d.pooledConn != nil {
		d.pooledConn.errored = true
	}
	return d.u.moduleError(dataErr)
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
		if d.pooledConn != nil {
			d.pooledConn.errored = true
		}
		modErr := d.u.moduleError(err)
		for _, rcpt := range d.rcpts[rcptIndx:] {
			sc.SetStatus(rcpt, modErr)
		}
	}
}

func (d *delivery) Abort(ctx context.Context) error {
	return d.conn.Close()
}

func (d *delivery) Commit(ctx context.Context) error {
	if d.pooledConn != nil {
		d.pooledConn.transactions++
		d.pooledConn.lastUse = time.Now()
		if d.pooledConn.Usable() {
			d.u.pool.Return(d.poolKey, d.pooledConn)
			return nil
		}
	}
	return d.conn.Close()
}

func init() {
	modules.Register("target.smtp", New)
	modules.Register("target.lmtp", New)
}
