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
	"github.com/emersion/go-sasl"
	"github.com/foxcpp/maddy/framework/config"
	"github.com/foxcpp/maddy/framework/exterrors"
	"github.com/foxcpp/maddy/framework/module"
)

// saslConfig holds both a factory for creating SASL clients and an identity
// extractor used to compute the pool key for connection reuse. The identity
// function returns a stable string that uniquely identifies the SASL
// credentials for a given message (e.g. "plain:user", "forward:alice").
type saslConfig struct {
	factory  func(msgMeta *module.MsgMetadata) (sasl.Client, error)
	identity func(msgMeta *module.MsgMetadata) string
}

// saslAuthDirective returns a saslConfig used to create sasl.Client
// for use in outbound connections and to extract the SASL identity for
// connection pool keying.
//
// Authentication information of the current client should be passed in arguments.
func saslAuthDirective(_ *config.Map, node config.Node) (interface{}, error) {
	if len(node.Children) != 0 {
		return nil, config.NodeErr(node, "can't declare a block here")
	}
	if len(node.Args) == 0 {
		return nil, config.NodeErr(node, "at least one argument required")
	}
	switch node.Args[0] {
	case "off":
		return &saslConfig{
			factory:  nil,
			identity: func(*module.MsgMetadata) string { return "" },
		}, nil
	case "forward":
		if len(node.Args) > 1 {
			return nil, config.NodeErr(node, "no additional arguments required")
		}
		return &saslConfig{
			factory: func(msgMeta *module.MsgMetadata) (sasl.Client, error) {
				if msgMeta.Conn == nil || msgMeta.Conn.AuthUser == "" || msgMeta.Conn.AuthPassword == "" {
					return nil, &exterrors.SMTPError{
						Code:         530,
						EnhancedCode: exterrors.EnhancedCode{5, 7, 0},
						Message:      "Authentication is required",
						TargetName:   "target.smtp",
						Reason:       "Credentials forwarding is requested but the client is not authenticated",
					}
				}
				return sasl.NewPlainClient("", msgMeta.Conn.AuthUser, msgMeta.Conn.AuthPassword), nil
			},
			identity: func(msgMeta *module.MsgMetadata) string {
				if msgMeta != nil && msgMeta.Conn != nil && msgMeta.Conn.AuthUser != "" {
					return "forward:" + msgMeta.Conn.AuthUser
				}
				return "forward:"
			},
		}, nil
	case "plain", "login":
		if len(node.Args) != 3 {
			return nil, config.NodeErr(node, "two additional arguments required (username, password)")
		}
		mech := node.Args[0]
		user := node.Args[1]
		pass := node.Args[2]
		return &saslConfig{
			factory: func(*module.MsgMetadata) (sasl.Client, error) {
				if mech == "plain" {
					return sasl.NewPlainClient("", user, pass), nil
				}
				if mech == "login" {
					return sasl.NewLoginClient(user, pass), nil
				}
				return nil, config.NodeErr(node, "unknown authentication mechanism: %s", mech)
			},
			identity: func(*module.MsgMetadata) string {
				return mech + ":" + user
			},
		}, nil
	case "external":
		if len(node.Args) > 1 {
			return nil, config.NodeErr(node, "no additional arguments required")
		}
		return &saslConfig{
			factory: func(*module.MsgMetadata) (sasl.Client, error) {
				return sasl.NewExternalClient(""), nil
			},
			identity: func(*module.MsgMetadata) string {
				return "external"
			},
		}, nil
	default:
		return nil, config.NodeErr(node, "unknown authentication mechanism: %s", node.Args[0])
	}
}
