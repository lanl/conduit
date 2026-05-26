// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"

	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// cpCmd represents the cp command
var cpCmd = &cobra.Command{
	Use:   "cp SOURCE... DESTINATION",
	Short: "copy files and directories",
	Long:  `Copy SOURCE to DEST, or multiple SOURCE(s) to DIRECTORY`,
	Args: func(cmd *cobra.Command, args []string) error {
		// check that there are at least two arguments
		if err := cobra.MinimumNArgs(2)(cmd, args); err != nil {
			return err
		}

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
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

		action := proto.Action_COPY
		recursive, err := cmd.Flags().GetBool("recursive")
		if err != nil {
			fmt.Printf("failed to get recursive flag: %v\n", err)
			os.Exit(1)
		}
		if recursive {
			action = proto.Action_RECURSIVE_COPY
		}

		skipValidation, err := cmd.Flags().GetBool("skip-validation")
		if err != nil {
			fmt.Printf("failed to get skip-validation flag: %v\n", err)
			os.Exit(1)
		}

		skipStat, err := cmd.Flags().GetBool("skip-stat")
		if err != nil {
			logger.Debugf("failed to get skip-stat flag: %v", err)
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

		td, err := client.StartTransfer(action, src, dst, skipValidation, skipStat, ps, validateOnly, providedUser, comment)
		if err != nil {
			fmt.Printf("request failed: %v\n", err)
			os.Exit(1)
		} else {
			if !quiet {
				fmt.Printf("successfully submitted transfer: %v\n", td.GetTransferID())
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
	},
}

func init() {
	cpCmd.Flags().BoolVarP(&quiet, "quiet", "q", false, "Reduce command output to only a transferID")
	cpCmd.Flags().BoolP("recursive", "r", false, "Copy directories recursively")
	cpCmd.Flags().BoolP("skip-validation", "s", false, "Skip waiting for validation to succeed")
	cpCmd.Flags().Bool("skip-stat", false, "Skip stating sources and destinations, this is not recommended")
	cpCmd.Flags().BoolP("background", "b", false, "Submit a conduit transfer without watching it progress to completion")
	cpCmd.Flags().StringVar(&providedUser, "user", "", "The user to start the transfer as. Requires an admin cert & key to be provided")
	cpCmd.Flags().String("comment", "", "A comment for the transfer. Used by conduit services. Requires an admin or service cert & key to be provided")
	cpCmd.Flags().Bool("validate-only", false, "Do not transfer any data, just run validation")

	// test mode flags. Should not be used in production
	cpCmd.Flags().String("pause", proto.TransferState_TRANSFER_NONE.String(), "specify a lease state to pause at for test mode")

	cpCmd.Flags().MarkHidden("skip-stat")
	cpCmd.Flags().MarkHidden("comment")
	cpCmd.Flags().MarkHidden("pause")

	RootCmd.AddCommand(cpCmd)
}
