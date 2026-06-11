package workflow

import (
	"github.com/spf13/cobra"

	"github.com/galgotech/heddle-lang/internal/dev/scaffold"
)

var InitCmd = &cobra.Command{
	Use:   "init <namespace>/<project_name>",
	Short: "Initialize a new Heddle project structure",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		projectName := args[0]
		s := scaffold.NewScaffoldService()
		return s.Init(projectName)
	},
}
