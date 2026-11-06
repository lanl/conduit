// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"

	proto "github.com/lanl/conduit/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/encoding/protojson"
)

var (
	serverCmdQueue bool
)

var serverCmd = &cobra.Command{
	Use:    "server",
	Short:  "control the conduit server state",
	Long:   `This subcommand allows an admin to control the conduit server state`,
	Hidden: true,
}

var serverStartCmd = &cobra.Command{
	Use:    "start",
	Short:  "starts the conduit server",
	Long:   `This subcommand starts the conduit server so it will start progressing transfers again`,
	Hidden: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
		}

		client, err := newCLIClient(cmd, logger, quiet)
		if err != nil {
			return fmt.Errorf("failed to create client: %v", err)
		}
		defer client.Close()

		resp, err := client.ServerControl(proto.ServerControlAction_SERVER_CONTROL_START)
		if err != nil {
			return fmt.Errorf("server start request failed: %v", err)
		} else {
			fmt.Printf("successfully started conduit server. current state: %s\n", resp.GetServerState())
		}

		return nil
	},
}

var serverDrainCmd = &cobra.Command{
	Use:    "drain",
	Short:  "drains the conduit server",
	Long:   `This subcommand drains the conduit server so it will progress currently queued transfers but will not progress any that are newly submitted. Note that this only works if all conduit instances for a cluster are draining. Use 'stop' to safely stop a single conduit instance`,
	Hidden: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
		}

		client, err := newCLIClient(cmd, logger, quiet)
		if err != nil {
			return fmt.Errorf("failed to create client: %v", err)
		}
		defer client.Close()

		resp, err := client.ServerControl(proto.ServerControlAction_SERVER_CONTROL_DRAIN)
		if err != nil {
			return fmt.Errorf("server drain request failed: %v", err)
		} else {
			fmt.Printf("successfully started draining conduit server. current state: %s\n", resp.GetServerState())
		}

		return nil
	},
}

var serverStatusCmd = &cobra.Command{
	Use:    "status",
	Short:  "get conduit server state",
	Long:   `This subcommand will retrieve the current state of the conduit server`,
	Hidden: false,
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
		}

		client, err := newCLIClient(cmd, logger, quiet)
		if err != nil {
			return fmt.Errorf("failed to create client: %v", err)
		}
		defer client.Close()

		resp, err := client.ServerControl(proto.ServerControlAction_SERVER_CONTROL_STATUS)
		if err != nil {
			fmt.Printf("server status request failed: %v\n", err)
		} else {
			fmt.Printf("conduit server state: %s\n", resp.GetServerState())
		}

		schedResp, err := client.SchedulerInfo()
		if err != nil {
			return fmt.Errorf("server scheduler request failed: %v", err)
		} else {
			mo := protojson.MarshalOptions{
				Multiline:       true,
				EmitUnpopulated: true,
			}

			if !serverCmdQueue {
				for _, s := range schedResp.GetSchedulers() {
					s.Queue = []*proto.SchedulerJob{}
				}
			}

			srBytes, err := mo.Marshal(schedResp)
			if err != nil {
				fmt.Printf("failed to marshal scheduler info: %v\n", err)
			}

			fmt.Printf("conduit scheduler state:\n%+v\n", string(srBytes))
		}

		return nil
	},
}

func init() {
	RootCmd.AddCommand(serverCmd)
	serverCmd.AddCommand(serverDrainCmd)
	serverCmd.AddCommand(serverStartCmd)
	serverCmd.AddCommand(serverStatusCmd)

	serverStatusCmd.Flags().BoolVar(&serverCmdQueue, "queue", false, "Display the entire scheduler queue")
}
