package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/server"
	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var cpCfgFile string

var cpCmd = &cobra.Command{
	Use:   "cp",
	Short: "Start the Heddle Control Plane",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_CP", cpCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		if err := logger.Init(logger.Config{Development: true}); err != nil {
			panic(err)
		}
		defer logger.Sync()

		port := viper.GetInt("port")
		logger.L().Info("Heddle Control Plane starting",
			zap.Int("port", port),
			zap.String("standard", "Apache Arrow Flight"))

		server.StartServer(port)
	},
}

func init() {
	cpCmd.Flags().StringVar(&cpCfgFile, "config", "", "config file (default is ./heddle-cp.yaml)")
}
