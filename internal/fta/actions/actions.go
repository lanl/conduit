package actions

import (
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/anypb"
)

const (
	Action_COPY     = "CONDUIT_COPY"
	Action_MOVE     = "CONDUIT_MOVE"
	RecursiveFlag   = "recursive"
	OmitMissingFlag = "omit-missing"
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
			},
			Command: cpCmd,
		},
		{
			Action: Action_MOVE,
			Options: map[string]func(*cobra.Command) (*anypb.Any, error){
				RecursiveFlag:   recursiveOption,
				OmitMissingFlag: omitMissingOption,
			},
			Command: mvCmd,
		},
	}
}
