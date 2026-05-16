package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/galgotech/heddle-lang/cmd/cluster"
	"github.com/galgotech/heddle-lang/cmd/dev"
	"github.com/galgotech/heddle-lang/cmd/inspect"
	"github.com/galgotech/heddle-lang/cmd/local"
	"github.com/galgotech/heddle-lang/cmd/run"
	"github.com/galgotech/heddle-lang/cmd/workflow"
	"github.com/galgotech/heddle-lang/internal/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var rootCmd = &cobra.Command{
	Use:   "heddle",
	Short: "Heddle is a strictly-typed, domain-specific programming language for data orchestration",
	Long: `Heddle Lang provides a high-performance orchestration engine with zero-copy data routing
and a strictly-typed DSL. This CLI tool manages all components of the Heddle ecosystem.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		cfgFile, _ := cmd.Flags().GetString("config")
		cfg, err := config.LoadHeddleConfig(cfgFile)
		if err != nil {
			return err
		}

		logLevel, _ := cmd.Flags().GetString("log-level")
		if !cmd.Flags().Changed("log-level") && cfg != nil && cfg.Log.Level != "" {
			logLevel = cfg.Log.Level
		}

		logFormat, _ := cmd.Flags().GetString("log-format")
		if !cmd.Flags().Changed("log-format") && cfg != nil && cfg.Log.Format != "" {
			logFormat = cfg.Log.Format
		}

		return logger.Init(logger.Config{
			Level:       logLevel,
			Encoding:    logFormat,
			Development: logLevel == "debug",
		})

	},
	CompletionOptions: cobra.CompletionOptions{
		DisableDefaultCmd: true,
	},
}

func init() {
	rootCmd.PersistentFlags().String("config", "", "path to heddle.yaml config file")
	rootCmd.PersistentFlags().String("log-level", "info", "log level (debug, info, warn, error)")
	rootCmd.PersistentFlags().String("log-format", "console", "log format (console, json)")

	rootCmd.AddCommand(run.RunCmd)
	rootCmd.AddCommand(workflow.WorkflowCmd)
	rootCmd.AddCommand(inspect.InspectCmd)
	rootCmd.AddCommand(local.LocalCmd)
	rootCmd.AddCommand(cluster.ClusterCmd)
	rootCmd.AddCommand(dev.DevCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
