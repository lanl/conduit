// Copyright 2026. Triad National Security, LLC. All rights reserved.

package ftacmd

import (
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
		log, it, em, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(it, em, proto.SchedulerCommand_VALIDATION)

		pErr, err, expiryQuit := fta.StartPluginETCD(log, proto.SchedulerCommand_VALIDATION, it, nodeList, em)
		defer func() {
			close(expiryQuit)
		}()

		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start validation plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_VALIDATION, it, em, errs, nil, proto.DestInfo_DEST_NONE)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		pluginData, destInfo, errs := fta.StartPluginValidate(log, it, em, nodeList)
		if len(errs.Errors) > 0 {
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_VALIDATION, it, em, errs, pluginData, destInfo)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		pErr, err = fta.CompletePluginETCD(log, proto.SchedulerCommand_VALIDATION, it, em, pluginData, destInfo, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete validation plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_VALIDATION, it, em, errs, pluginData, destInfo)
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
		log, it, em, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(it, em, proto.SchedulerCommand_SETUP)

		pErr, err, expiryQuit := fta.StartPluginETCD(log, proto.SchedulerCommand_SETUP, it, nodeList, em)
		defer func() {
			close(expiryQuit)
		}()

		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start setup plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_SETUP, it, em, errs, nil, proto.DestInfo_DEST_NONE)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		pluginData, errs := fta.StartPluginSetup(log, it, em, nodeList)
		if len(errs.Errors) > 0 {
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_SETUP, it, em, errs, nil, proto.DestInfo_DEST_NONE)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		pErr, err = fta.CompletePluginETCD(log, proto.SchedulerCommand_SETUP, it, em, pluginData, proto.DestInfo_DEST_NONE, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete setup plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_SETUP, it, em, errs, nil, proto.DestInfo_DEST_NONE)
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
		log, it, em, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(it, em, proto.SchedulerCommand_TRANSFER)

		pErr, err, expiryQuit := fta.StartPluginETCD(log, proto.SchedulerCommand_TRANSFER, it, nodeList, em)
		defer func() {
			close(expiryQuit)
		}()

		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start transfer plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_TRANSFER, it, em, errs, nil, proto.DestInfo_DEST_NONE)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		errs := fta.StartPluginTransfer(log, it, em, nodeList)
		if len(errs.Errors) > 0 {
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_TRANSFER, it, em, errs, nil, proto.DestInfo_DEST_NONE)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		pErr, err = fta.CompletePluginETCD(log, proto.SchedulerCommand_TRANSFER, it, em, nil, proto.DestInfo_DEST_NONE, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete transfer plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_TRANSFER, it, em, errs, nil, proto.DestInfo_DEST_NONE)
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
		log, it, em, nodeList := fta.FTAInit(debug)

		go fta.ListenForKill(it, em, proto.SchedulerCommand_TEARDOWN)

		pErr, err, expiryQuit := fta.StartPluginETCD(log, proto.SchedulerCommand_TEARDOWN, it, nodeList, em)
		defer func() {
			close(expiryQuit)
		}()
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to start teardown plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_TEARDOWN, it, em, errs, nil, proto.DestInfo_DEST_NONE)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		errs := fta.StartPluginTeardown(log, it, em, nodeList)
		if len(errs.Errors) > 0 {
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_TEARDOWN, it, em, errs, nil, proto.DestInfo_DEST_NONE)
			if err != nil {
				log.Fatalf("failed to set transfer to error state in etcd: %v", err)
			}
			return
		}

		pErr, err = fta.CompletePluginETCD(log, proto.SchedulerCommand_TEARDOWN, it, em, nil, proto.DestInfo_DEST_NONE, errs)
		if err != nil {
			errs := errToErrs(fmt.Errorf("failed to complete teardown plugin in etcd: %v", err), pErr)
			_, err := fta.ErrorPluginETCD(log, proto.SchedulerCommand_TEARDOWN, it, em, errs, nil, proto.DestInfo_DEST_NONE)
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

	RootCmd.PersistentFlags().String("ca-cert", DefaultCACertLocation, "location of the ca cert .pem file")
	RootCmd.PersistentFlags().IPSliceVar(&etcdIPs, "etcd-ip", DefaultETCDIPNet, "ip address(es) of etcd")
	RootCmd.PersistentFlags().IntSliceVar(&etcdPorts, "etcd-port", DefaultETCDPort, "client port(s) of etcd")

	RootCmd.PersistentFlags().BoolP("encoded", "e", false, "Use if stdin is base64 encoded")

	viper.BindPFlag(defaults.ConfigInternalCACertKey, RootCmd.PersistentFlags().Lookup("ca-cert"))

	RootCmd.AddCommand(validateCmd)
	RootCmd.AddCommand(setupCmd)
	RootCmd.AddCommand(transferCmd)
	RootCmd.AddCommand(teardownCmd)
}
