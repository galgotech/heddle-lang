package dev

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/dap"
	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var dapCfgFile string

// DapCmd starts the Heddle Debug Adapter
var DapCmd = &cobra.Command{
	Use:   "dap",
	Short: "Start the Heddle Debug Adapter",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_DAP", dapCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		log := logger.L()
		cpAddr := viper.GetString("control-plane-addr")
		if cpAddr == "" {
			cpAddr = "localhost:50051"
		}

		server := dap.NewServer(log, viper.GetString("addr"), cpAddr)

		if viper.GetBool("server") {
			if err := server.Start(context.Background()); err != nil {
				log.Fatal("DAP server failed", zap.Error(err))
			}
		} else {
			if err := server.StartStdio(context.Background(), os.Stdin, os.Stdout); err != nil {
				log.Fatal("DAP stdio failed", zap.Error(err))
			}
		}
	},
}

func init() {
	DapCmd.PersistentFlags().StringVar(&dapCfgFile, "config", "", "config file (default is ./heddle-dap.yaml)")
	DapCmd.Flags().Bool("server", false, "Start in server mode")
	DapCmd.Flags().String("addr", "localhost:4711", "Address to listen on in server mode")

	viper.BindPFlag("server", DapCmd.Flags().Lookup("server"))
	viper.BindPFlag("addr", DapCmd.Flags().Lookup("addr"))
}
