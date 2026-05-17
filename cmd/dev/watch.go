package dev

import (
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/galgotech/heddle-lang/cmd/local"
	"github.com/galgotech/heddle-lang/internal/dev/maestro"
)

var WatchCmd = &cobra.Command{
	Use:   "watch [project-path]",
	Short: "Start the local development orchestrator with hot-reload",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// 1. Start local services (Control Plane, Core Worker, Std Plugins)
		if err := local.StartLocalServices(cmd.Context()); err != nil {
			return err
		}

		projectPath := "."
		if len(args) > 0 {
			projectPath = args[0]
		}

		absPath, err := filepath.Abs(projectPath)
		if err != nil {
			return err
		}

		// 2. Start Maestro for hot-reload of SDK workers
		m, err := maestro.NewMaestro(absPath)
		if err != nil {
			return err
		}
		return m.Run(cmd.Context())
	},
}
