// Copyright 2026. Triad National Security, LLC. All rights reserved.

package clicmd

import (
	"fmt"

	"github.com/google/uuid"
	proto "github.com/lanl/conduit/api"
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

		pTID := args[0]
		pPauseState := args[1]

		transferID, err := uuid.Parse(pTID)
		if err != nil {
			return fmt.Errorf("failed to parse transferID[%s]: %v", pTID, err)
		}

		ps := proto.TransferState_TRANSFER_NONE
		if psi, ok := proto.TransferState_value[pPauseState]; ok {
			ps = proto.TransferState(psi)
		} else {
			return fmt.Errorf("not a valid pause state: %s", pPauseState)
		}

		td, err := client.PauseTransfer(transferID, ps)
		if err != nil {
			return fmt.Errorf("pause request failed: %v", err)
		} else {
			// check if we just paused a transfer at a point that it's already past
			if td.GetState() > ps && ps != proto.TransferState_TRANSFER_NONE {
				fmt.Printf("successfully submitted transfer pause, but transfer is already past that state: %v\n", td.GetTransferID())
			} else {
				fmt.Printf("successfully submitted transfer pause: %v\n", td.GetTransferID())
			}
		}

		return nil
	},
}

func init() {

	RootCmd.AddCommand(pauseCmd)
}
