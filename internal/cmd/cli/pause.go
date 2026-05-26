// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
	"github.com/lanl/conduit/defaults"
	"github.com/lanl/conduit/internal/cli/client"
	"github.com/lanl/conduit/internal/cli/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// pauseCmd represents the pause command
var pauseCmd = &cobra.Command{
	Use:    "pause TRANSFER_ID PAUSE_STATE",
	Short:  "pause a transfer (test mode only)",
	Long:   `This subcommand pauses transfers at a specified pause state`,
	Hidden: true,
	Args: func(cmd *cobra.Command, args []string) error {
		// Optionally run one of the validators provided by cobra
		if err := cobra.ExactArgs(2)(cmd, args); err != nil {
			return err
		}

		// check if transferID is a valid uuid
		_, err := uuid.Parse(args[0])
		if err != nil {
			return fmt.Errorf("invalid uuid specified %v: %v", args[0], err)
		}

		// check if pause state is a valid transfer state
		if _, ok := proto.TransferState_value[args[1]]; !ok {
			tsList := []string{}
			for i := int32(0); i < int32(len(proto.TransferState_name)); i++ {
				tsList = append(tsList, proto.TransferState_name[i])
			}
			return fmt.Errorf("invalid pause state specified: %s\npossible pause states: %v", args[1], tsList)
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
		pTID := args[0]
		pPauseState := args[1]

		transferID, err := uuid.Parse(pTID)
		if err != nil {
			fmt.Printf("failed to parse transferID[%s]: %v\n", pTID, err)
			os.Exit(1)
		}

		ps := proto.TransferState_TRANSFER_NONE
		if psi, ok := proto.TransferState_value[pPauseState]; ok {
			ps = proto.TransferState(psi)
		} else {
			fmt.Printf("not a valid pause state: %s\n", pPauseState)
			os.Exit(1)
		}

		td, err := client.PauseTransfer(transferID, ps)
		if err != nil {
			fmt.Printf("pause request failed: %v\n", err)
			os.Exit(1)
		} else {
			// check if we just paused a transfer at a point that it's already past
			if td.GetState() > ps && ps != proto.TransferState_TRANSFER_NONE {
				fmt.Printf("successfully submitted transfer pause, but transfer is already past that state: %v\n", td.GetTransferID())
			} else {
				fmt.Printf("successfully submitted transfer pause: %v\n", td.GetTransferID())
			}
		}
	},
}

func init() {

	RootCmd.AddCommand(pauseCmd)
}
