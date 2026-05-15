package dev

import (
	"github.com/spf13/cobra"

	"github.com/galgotech/heddle-lang/cmd/local"
	"github.com/galgotech/heddle-lang/internal/services/dev/maestro"
)

var WatchCmd = &cobra.Command{
	Use:   "watch",
	Short: "Start the local development orchestrator with hot-reload",
	RunE: func(cmd *cobra.Command, args []string) error {
		// 1. Start local services (Control Plane, Core Worker, Std Plugins)
		if err := local.StartLocalServices(cmd.Context()); err != nil {
			return err
		}

		// 2. Start Maestro for hot-reload of SDK workers
		m, err := maestro.NewMaestro()
		if err != nil {
			return err
		}
		return m.Run(cmd.Context())
	},
}
