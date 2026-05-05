package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/services/control-plane/pkg/server"
)

var (
	cfgFile string
)

// rootCmd defines the entry point for the Heddle Control Plane (the "Brain").
// It initializes the global configuration, logger, and the high-performance Arrow Flight server.
var rootCmd = &cobra.Command{
	Use:   "heddle-control-plane",
	Short: "Heddle Control Plane manages workers and workflow execution",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		// Bootstrap configuration from environment variables and config files.
		return initializeConfig()
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize the global logger with development-level verbosity.
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		port := viper.GetInt("port")
		logger.L().Info("Heddle Control Plane starting",
			zap.Int("port", port),
			zap.String("standard", "Apache Arrow Flight"))

		// Start the gRPC/Flight server to begin accepting worker registrations and workflow submissions.
		server.StartServer(port)
	},
}

// initializeConfig loads the configuration into the global Viper registry.
func initializeConfig() error {
	return config.Init("HEDDLE_CP", cfgFile)
}

func main() {
	// Execute the CLI root command.
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	// CLI flag definitions and Viper bindings for unified configuration access.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-cp.yaml)")
	rootCmd.Flags().IntP("port", "p", 50051, "Port to listen on for gRPC/Flight traffic")

	viper.BindPFlag("port", rootCmd.Flags().Lookup("port"))
}
