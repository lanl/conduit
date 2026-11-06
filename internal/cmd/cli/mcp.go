// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/logger"
	conduitmcp "github.com/lanl/conduit/internal/mcp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var transport string
var address string

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Run an MCP server using the current user's Conduit credentials",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, _ []string) error {
		if transport != "stdio" && transport != "http" {
			return fmt.Errorf("invalid MCP transport %q (expected stdio or http)", transport)
		}

		// The normal CLI uses a logrus logger. MCP's stdio transport needs
		// every log destination, including the MCP package logger, on stderr.
		cliLog := logrus.New()
		cliLog.SetOutput(cmd.ErrOrStderr())
		if debug {
			cliLog.SetLevel(logrus.DebugLevel)
		}
		mcpLog := logger.NewConduitLogger(cliLog.GetLevel(), "MCP CLI:")
		outputLog, ok := any(mcpLog).(interface{ SetOutput(io.Writer) })
		if !ok {
			return fmt.Errorf("MCP logger must support SetOutput so it can write to stderr")
		}
		outputLog.SetOutput(cmd.ErrOrStderr())

		cert, key, err := cliClientCredentials(cmd)
		if err != nil {
			return err
		}
		cliLog.Debugf("using CLI client certificate [%s] and key [%s]", cert, key)

		// This is the same dialer used by client.NewClient for other CLI
		// commands. NonInteractive skips the configured kinit helper: it
		// could otherwise steal MCP stdin or print into protocol stdout.f
		conn, grpcClient, err := client.DialClient(cliLog, client.DialOptions{
			CertPath:       cert,
			KeyPath:        key,
			Quiet:          true,
			NonInteractive: true,
		})
		if err != nil {
			return fmt.Errorf("failed to create Conduit CLI gRPC client: %w", err)
		}

		srv, err := conduitmcp.CreateLocalMCPServer(mcpLog, conn, grpcClient)
		if err != nil {
			_ = conn.Close()
			return err
		}

		// The MCP server owns and closes conn after its Run/Serve returns.
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if transport == "stdio" {
			cmd.SetOut(cmd.ErrOrStderr())
			return srv.StartLocalMCPStdio(ctx)
		}

		// HTTP transport can use stdout normally.
		return srv.StartLocalMCPHTTP(ctx, address, cmd.OutOrStdout())
	},
}

func init() {
	mcpCmd.Flags().StringVar(&transport, "transport", "stdio", "MCP transport: stdio or http")
	mcpCmd.Flags().StringVar(&address, "addr", "127.0.0.1:0", "MCP HTTP listen address (numeric loopback only)")

	RootCmd.AddCommand(mcpCmd)

}
