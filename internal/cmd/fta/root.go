// Copyright 2026. Triad National Security, LLC. All rights reserved.

package ftacmd

import (
	"context"
	"fmt"
	"net"
	"os"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/fta"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	cfgFile   string
	debug     bool
	etcdIPs   []net.IP
	etcdPorts []int
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "conduit-fta",
	Short: "run conduit-fta commands",
	Long:  `This is conduit-fta`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if len(etcdIPs) != len(etcdPorts) {
			return fmt.Errorf("must provide equal numbers of etcd-ip and etcd-port")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		logrus.Error(err)
		os.Exit(1)
	}
}

var validateCmd = &cobra.Command{
	Use:   proto.SchedulerCommand_VALIDATION.String(),
	Short: "start the validation process",
	Long:  `This subcommand starts the validation process`,
	Run: func(cmd *cobra.Command, args []string) {
		log, t, client, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(t, client)

		ctx, expiryQuit := context.WithCancel(context.Background())
		pErr, err := client.StartPlugin(ctx)
		defer func() {
			expiryQuit()
		}()

		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start validation plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		pluginData, destInfo, errs := fta.StartPluginValidate(log, t, nodeList)
		if len(errs.Errors) > 0 {
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, pluginData, destInfo, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		ctx = context.Context(context.Background())
		pErr, err = client.CompletePlugin(ctx, proto.SchedulerCommand_VALIDATION, pluginData, destInfo, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete validation plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, pluginData, destInfo, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}
	},
}

// setupCmd represents a stage in command
var setupCmd = &cobra.Command{
	Use:   proto.SchedulerCommand_SETUP.String(),
	Short: "start the stage in process",
	Long:  `This subcommand starts a stage in process`,
	Run: func(cmd *cobra.Command, args []string) {
		log, t, client, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(t, client)

		ctx, expiryQuit := context.WithCancel(context.Background())
		pErr, err := client.StartPlugin(ctx)
		defer func() {
			expiryQuit()
		}()

		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start setup plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return

		}

		pluginData, errs := fta.StartPluginSetup(log, t, client, nodeList)
		if len(errs.Errors) > 0 {
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		ctx = context.Context(context.Background())
		pErr, err = client.CompletePlugin(ctx, proto.SchedulerCommand_SETUP, pluginData, proto.DestInfo_DEST_NONE, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete setup plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}
	},
}

// transferCmd represents the transfer command
var transferCmd = &cobra.Command{
	Use:   proto.SchedulerCommand_TRANSFER.String(),
	Short: "start a pftool transfer",
	Long:  `This subcommand starts a transfer using pftool`,
	Run: func(cmd *cobra.Command, args []string) {
		log, t, client, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(t, client)

		ctx, expiryQuit := context.WithCancel(context.Background())
		pErr, err := client.StartPlugin(ctx)
		defer func() {
			expiryQuit()
		}()

		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start transfer plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		errs := fta.StartPluginTransfer(log, t, client, nodeList)
		if len(errs.Errors) > 0 {
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		ctx = context.Context(context.Background())
		pErr, err = client.CompletePlugin(ctx, proto.SchedulerCommand_TRANSFER, nil, proto.DestInfo_DEST_NONE, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete transfer plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}
	},
}

// teardownCmd represents a stage in command
var teardownCmd = &cobra.Command{
	Use:   proto.SchedulerCommand_TEARDOWN.String(),
	Short: "start the stage out process",
	Long:  `This subcommand starts a stage in process`,
	Run: func(cmd *cobra.Command, args []string) {
		log, t, client, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(t, client)

		ctx, expiryQuit := context.WithCancel(context.Background())
		pErr, err := client.StartPlugin(ctx)
		defer func() {
			expiryQuit()
		}()

		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start teardown plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		errs := fta.StartPluginTeardown(log, t, client, nodeList)
		if len(errs.Errors) > 0 {
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		ctx = context.Context(context.Background())
		pErr, err = client.CompletePlugin(ctx, proto.SchedulerCommand_TEARDOWN, nil, proto.DestInfo_DEST_NONE, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete teardown plugin in etcd: %v", err), pErr)
			ctx := context.Context(context.Background())
			_, err := client.FailPlugin(ctx, nil, proto.DestInfo_DEST_NONE, errs)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}
	},
}

func init() {
	cobra.OnInitialize(func() { initConfig(cfgFile) })

	// global flags
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", fmt.Sprintf("config file (default is %s%s.%s)", DefaultConfigLocation, ConfigName, ConfigType))
	RootCmd.PersistentFlags().BoolVarP(&debug, "debug", "d", false, "enable debugging")

	viper.BindPFlag(defaults.ConfigInternalCACertKey, RootCmd.PersistentFlags().Lookup("ca-cert"))

	RootCmd.AddCommand(validateCmd)
	RootCmd.AddCommand(setupCmd)
	RootCmd.AddCommand(transferCmd)
	RootCmd.AddCommand(teardownCmd)
}
