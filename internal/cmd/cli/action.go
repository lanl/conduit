// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"
	"strings"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/lanl/conduit/internal/fta/actions"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/protobuf/types/known/anypb"
)

func getCommands() {
	pluginActions := actions.GetActions()

	for _, pa := range pluginActions {
		attachRunCommand(pa)

		RootCmd.AddCommand(pa.Command)
	}
}

func attachRunCommand(pluginAction *actions.PluginAction) {
	pluginAction.Command.Run = func(cmd *cobra.Command, args []string) {
		logger := logrus.New()
		if debug {
			logger.SetLevel(logrus.DebugLevel)
			logger.Debugf("loaded cli config from: %v", viper.ConfigFileUsed())
		}

		clientCertKeyBundle, err := cmd.Flags().GetString("cert-key-bundle")
		if err != nil {
			fmt.Printf("failed to get cert-key-bundle flag: %v\n", err)
			os.Exit(1)
		}
		clientCert, clientKey, err := util.GetUserCertAndKey(viper.GetString(defaults.ConfigClientCertKey), viper.GetString(defaults.ConfigClientKeyKey), clientCertKeyBundle, defaults.DefaultBundlePath)
		if err != nil {
			fmt.Printf("failed to get client cert and key: %v\n", err)
			os.Exit(1)
		}
		logger.Debugf("using user cert [%v] and key [%v]", clientCert, clientKey)

		client, err := client.NewClient(logger, quiet, clientCert, clientKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		src := args[:len(args)-1]
		dst := args[len(args)-1]

		action := pluginAction.Action

		options := make(map[string]*anypb.Any)

		for optionName, optionGet := range pluginAction.Options {
			val, err := optionGet(cmd)
			if err != nil {
				fmt.Printf("%v\n", err)
				os.Exit(1)
			}
			options[optionName] = val
		}

		skipValidation, err := cmd.Flags().GetBool("skip-validation")
		if err != nil {
			fmt.Printf("failed to get skip-validation flag: %v\n", err)
			os.Exit(1)
		}

		pauseState, err := cmd.Flags().GetString("pause")
		if err != nil {
			if !quiet {
				fmt.Printf("failed to get pause flag: %v\n", err)
			}
		}

		comment, err := cmd.Flags().GetString("comment")
		if err != nil {
			if !quiet {
				fmt.Printf("failed to get comment: %v\n", err)
			}
		}

		workdir, err := cmd.Flags().GetString("work-dir")
		if err != nil {
			if !quiet {
				fmt.Printf("failed to get work-dir flag: %v\n", err)
			}
		}

		validateOnly := false
		validateOnly, err = cmd.Flags().GetBool("validate-only")
		if err != nil {
			fmt.Printf("failed to get skip-validation flag: %v\n", err)
			os.Exit(1)
		}

		ps := proto.TransferState_TRANSFER_NONE
		if psi, ok := proto.TransferState_value[pauseState]; ok {
			ps = proto.TransferState(psi)
		} else {
			fmt.Printf("an invalid pause state was provided: %v\n", pauseState)
			lsList := []string{}
			for i := int32(0); i < int32(len(proto.TransferState_name)); i++ {
				lsList = append(lsList, proto.TransferState_name[i])
			}
			fmt.Printf("possible pause states: %v\n", lsList)
			os.Exit(1)
		}

		if !quiet {
			doneChan := make(chan bool)
			go printErrantPaths(logger, client, doneChan)
			<-doneChan
		}

		td, err := client.StartTransfer(action, options, src, dst, skipValidation, ps, validateOnly, providedUser, comment, workdir)
		if err != nil {
			fmt.Printf("request failed: %v\n", err)
			// print any validation warnings
			for _, warning := range td.GetWarnings() {
				if strings.HasPrefix(warning, proto.SchedulerCommand_VALIDATION.String()) {
					fmt.Printf("Warning: %s\n", strings.TrimPrefix(warning, fmt.Sprintf("%s: ", proto.SchedulerCommand_VALIDATION.String())))
				}
			}
			os.Exit(1)
		} else {
			if !quiet {
				fmt.Printf("successfully submitted transfer: %v\n", td.GetTransferID())
				// print any validation warnings
				for _, warning := range td.GetWarnings() {
					if strings.HasPrefix(warning, proto.SchedulerCommand_VALIDATION.String()) {
						fmt.Printf("Warning: %s\n", strings.TrimPrefix(warning, fmt.Sprintf("%s: ", proto.SchedulerCommand_VALIDATION.String())))
					}
				}
			} else {
				fmt.Printf("%v\n", td.GetTransferID())
			}
		}

		background, err := cmd.Flags().GetBool("background")
		if err != nil {
			fmt.Printf("failed to get background flag: %v\n", err)
			os.Exit(1)
		}

		if !background && !skipValidation {
			watchCmd.Run(cmd, []string{td.GetTransferID()})
		}
	}

	pluginAction.Command.Flags().BoolVarP(&quiet, "quiet", "q", false, "Reduce command output to only a transferID")
	pluginAction.Command.Flags().BoolP("skip-validation", "s", false, "Skip waiting for validation to succeed")
	pluginAction.Command.Flags().Bool("skip-stat", false, "Skip stating sources and destinations, this is not recommended")
	pluginAction.Command.Flags().BoolP("background", "b", false, "Submit a conduit transfer without watching it progress to completion")
	pluginAction.Command.Flags().StringVar(&providedUser, "user", "", "The user to start the transfer as. Requires an admin cert & key to be provided")
	pluginAction.Command.Flags().String("comment", "", "A comment for the transfer. Used by conduit services. Requires an admin or service cert & key to be provided")
	pluginAction.Command.Flags().Bool("validate-only", false, "Do not transfer any data, just run validation")
	pluginAction.Command.Flags().String("work-dir", "", "Override the working directory. Used for path auto-completion")

	// test mode flags. Should not be used in production
	pluginAction.Command.Flags().String("pause", proto.TransferState_TRANSFER_NONE.String(), "specify a lease state to pause at for test mode")

	pluginAction.Command.Flags().MarkHidden("skip-stat")
	pluginAction.Command.Flags().MarkHidden("comment")
	pluginAction.Command.Flags().MarkHidden("pause")
	pluginAction.Command.Flags().MarkHidden("work-dir")

}

func init() {
	getCommands()
}
