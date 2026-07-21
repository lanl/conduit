package actions

import "github.com/spf13/cobra"

// mvCmd represents the mv command
var mvCmd = &cobra.Command{
	Use:   "mv SOURCE... DESTINATION",
	Short: "move files and directories",
	Long:  `Move SOURCE to DEST, or multiple SOURCE(s) to DIRECTORY`,
	Args: func(cmd *cobra.Command, args []string) error {
		// check that there are at least two arguments
		if err := cobra.MinimumNArgs(2)(cmd, args); err != nil {
			return err
		}

		return nil
	},
}

func init() {
	mvCmd.Flags().BoolP(RecursiveFlag, "r", false, "move directories recursively")
	mvCmd.Flags().Bool(OmitMissingFlag, false, "omit any sources that don't exist")
}
