package dev

import (
	"github.com/spf13/cobra"
)

// DevCmd is the root command for development and debugging tools.
var DevCmd = &cobra.Command{
	Use:   "dev",
	Short: "Development and debugging tools",
	Long:  `Development tools include the Heddle Language Server (LSP) and the Debug Adapter (DAP).`,
}

func init() {
	DevCmd.AddCommand(LspCmd)
	DevCmd.AddCommand(DapCmd)
}
