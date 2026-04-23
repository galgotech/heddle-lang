package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var (
	cfgFile string
)

var rootCmd = &cobra.Command{
	Use:   "heddle-control-plane",
	Short: "Heddle Control Plane manages workers and workflow execution",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig()
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		port := viper.GetInt("port")
		logger.L().Info("Heddle Control Plane starting", zap.Int("port", port))
		logger.L().Info("Arrow Flight server initializing")

		StartServer(port)
	},
}

func initializeConfig() error {
	return config.Init("HEDDLE_CP", cfgFile)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-cp.yaml)")
	rootCmd.Flags().IntP("port", "p", 50051, "Port to listen on")

	viper.BindPFlag("port", rootCmd.Flags().Lookup("port"))
}
