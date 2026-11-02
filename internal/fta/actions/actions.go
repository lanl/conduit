// Copyright 2026. Triad National Security, LLC. All rights reserved.

package actions

import (
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	Action_COPY     = "COPY"
	Action_MOVE     = "MOVE"
	RecursiveFlag   = "recursive"
	OmitMissingFlag = "omit-missing"
	ParentsFlag     = "parents"
)

type PluginAction struct {
	Command *cobra.Command
	Action  string
	Options map[string]func(*cobra.Command) (*anypb.Any, error)
}

func GetActions() []*PluginAction {

	return []*PluginAction{
		{
			Action: Action_COPY,
			Options: map[string]func(*cobra.Command) (*anypb.Any, error){
				RecursiveFlag:   recursiveOption,
				OmitMissingFlag: omitMissingOption,
				ParentsFlag:     parentsOption,
			},
			Command: cpCmd,
		},
		{
			Action: Action_MOVE,
			Options: map[string]func(*cobra.Command) (*anypb.Any, error){
				RecursiveFlag:   recursiveOption,
				OmitMissingFlag: omitMissingOption,
				ParentsFlag:     parentsOption,
			},
			Command: mvCmd,
		},
	}
}
