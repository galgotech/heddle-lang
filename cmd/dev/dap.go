package dev

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/galgotech/heddle-lang/internal/config"
	"github.com/galgotech/heddle-lang/internal/dap"
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
		cpAddr, _ := cmd.Flags().GetString("control-plane-addr")
		addr, _ := cmd.Flags().GetString("addr")

		l.Info("DAP server starting", logger.String("addr", addr), logger.String("control-plane-addr", cpAddr))

		server := dap.NewServer(l, addr, cpAddr)

		if err := server.StartStdio(context.Background(), os.Stdin, os.Stdout); err != nil {
			l.Fatal("DAP stdio failed", logger.Error(err))
		}
	},
}

func init() {
	DapCmd.PersistentFlags().StringVar(&dapCfgFile, "config", "", "config file (default is ./heddle-dap.yaml)")
	DapCmd.Flags().String("addr", "localhost:4711", "Address to listen on in server mode")
	DapCmd.Flags().String("log-path", "/tmp/heddle-dap.log", "Path to log file")
	DapCmd.Flags().String("control-plane-addr", "localhost:50051", "Address of the Heddle Control Plane")

	viper.BindPFlag("server", DapCmd.Flags().Lookup("server"))
	viper.BindPFlag("addr", DapCmd.Flags().Lookup("addr"))
	viper.BindPFlag("log-path", DapCmd.Flags().Lookup("log-path"))
	viper.BindPFlag("control-plane-addr", DapCmd.Flags().Lookup("control-plane-addr"))
}
