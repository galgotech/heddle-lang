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
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if err := config.Init("HEDDLE_DAP", dapCfgFile); err != nil {
			return err
		}

		logPath, err := cmd.Flags().GetString("log-path")
		if err != nil {
			return err
		}

		return logger.Init(logger.Config{
			OutputPaths: []string{logPath},
			Level:       "debug",
			Development: true,
		})
	},
	Run: func(cmd *cobra.Command, args []string) {
		l := logger.L()
		cpAddr := viper.GetString("control-plane-addr")
		if cpAddr == "" {
			cpAddr = "localhost:50051"
		}

		server := dap.NewServer(l, viper.GetString("addr"), cpAddr)

		if viper.GetBool("server") {
			if err := server.Start(context.Background()); err != nil {
				l.Fatal("DAP server failed", zap.Error(err))
			}
		} else {
			if err := server.StartStdio(context.Background(), os.Stdin, os.Stdout); err != nil {
				l.Fatal("DAP stdio failed", zap.Error(err))
			}
		}
	},
}

func init() {
	DapCmd.PersistentFlags().StringVar(&dapCfgFile, "config", "", "config file (default is ./heddle-dap.yaml)")
	DapCmd.Flags().Bool("server", false, "Start in server mode")
	DapCmd.Flags().String("addr", "localhost:4711", "Address to listen on in server mode")
	DapCmd.Flags().String("log-path", "/tmp/heddle-dap.log", "Path to log file")

	viper.BindPFlag("server", DapCmd.Flags().Lookup("server"))
	viper.BindPFlag("addr", DapCmd.Flags().Lookup("addr"))
	viper.BindPFlag("log-path", DapCmd.Flags().Lookup("log-path"))
}
