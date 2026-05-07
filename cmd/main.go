package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "heddle [file.he]",
	Short: "Heddle is a strictly-typed, domain-specific programming language for data orchestration",
	Long: `Heddle Lang provides a high-performance orchestration engine with zero-copy data routing
and a strictly-typed DSL. This CLI tool manages all components of the Heddle ecosystem.`,
	Args: cobra.MaximumNArgs(1),
}

func init() {
	// Root command (standalone) uses an optional file argument.
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
