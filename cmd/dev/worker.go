package dev

import (
	"github.com/spf13/cobra"

	"github.com/galgotech/heddle-lang/internal/dev/scaffold"
)

var WorkerCmd = &cobra.Command{
	Use:   "worker",
	Short: "Manage Heddle workers",
}

func init() {
	WorkerCmd.AddCommand(WorkerAddCmd)
	WorkerCmd.AddCommand(WorkerValidateCmd)
}

var WorkerAddCmd = &cobra.Command{
	Use:   "add <language> <namespace>/<worker_name>",
	Short: "Scaffold a new worker",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		language := args[0]
		fullName := args[1]
		s := scaffold.NewScaffoldService()
		return s.WorkerAdd(language, fullName)
	},
}

var WorkerValidateCmd = &cobra.Command{
	Use:   "validate",
	Short: "Validate the workers workspace structure",
	RunE: func(cmd *cobra.Command, args []string) error {
		s := scaffold.NewScaffoldService()
		_, err := s.WorkerValidate()
		return err
	},
}
