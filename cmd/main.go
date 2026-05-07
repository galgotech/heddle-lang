package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/galgotech/heddle-lang/cmd/cluster"
	"github.com/galgotech/heddle-lang/cmd/development"
	"github.com/galgotech/heddle-lang/cmd/local"
)

var rootCmd = &cobra.Command{
	Use:   "heddle",
	Short: "Heddle is a strictly-typed, domain-specific programming language for data orchestration",
	Long: `Heddle Lang provides a high-performance orchestration engine with zero-copy data routing
and a strictly-typed DSL. This CLI tool manages all components of the Heddle ecosystem.`,
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
}

func init() {
	// Root command (standalone) uses an optional file argument.
	rootCmd.AddCommand(cluster.ClusterCmd)
	rootCmd.AddCommand(development.DevelopmentCmd)
	rootCmd.AddCommand(local.LocalCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
