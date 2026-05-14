package dev

import (
	"github.com/spf13/cobra"

	devservice "github.com/galgotech/heddle-lang/internal/services/dev"
)

var InitCmd = &cobra.Command{
	Use:   "init <namespace>/<project_name>",
	Short: "Initialize a new Heddle project structure",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		projectName := args[0]
		s := devservice.NewScaffoldService()
		return s.Init(projectName)
	},
}
