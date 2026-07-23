// Copyright 2026. Triad National Security, LLC. All rights reserved.
package actions

import (
	"fmt"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
}

var recursiveOption = func(cmd *cobra.Command) (*anypb.Any, error) {
	r, err := cmd.Flags().GetBool(RecursiveFlag)
	if err != nil {
		return nil, fmt.Errorf("failed to get recursive flag: %v", err)
	}
	rAny, err := anypb.New(wrapperspb.Bool(r))
	if err != nil {
		return nil, fmt.Errorf("failed to convert recursive flag into a pb.any: %v", err)
	}

	return rAny, err
}

var omitMissingOption = func(cmd *cobra.Command) (*anypb.Any, error) {
	om, err := cmd.Flags().GetBool(OmitMissingFlag)
	if err != nil {
		return nil, fmt.Errorf("failed to get omit missing flag: %v", err)
	}
	omAny, err := anypb.New(wrapperspb.Bool(om))
	if err != nil {
		return nil, fmt.Errorf("failed to convert omit missing flag into a pb.any: %v", err)
	}

	return omAny, err
}

func init() {
	cpCmd.Flags().BoolP(RecursiveFlag, "r", false, "copy directories recursively")
	cpCmd.Flags().Bool(OmitMissingFlag, false, "omit any sources that don't exist")
}
