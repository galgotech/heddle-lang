package dev

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/lsp"
	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

var lspCfgFile string

type stdioRW struct {
	io.Reader
	io.Writer
}

func (stdioRW) Close() error {
	return nil
}

// LspCmd starts the Heddle Language Server
var LspCmd = &cobra.Command{
	Use:   "lsp",
	Short: "Start the Heddle Language Server",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if err := config.Init("HEDDLE_LSP", lspCfgFile); err != nil {
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
		server := lsp.NewServer(l, cpAddr)

		rw := stdioRW{cmd.InOrStdin(), cmd.OutOrStdout()}
		defer rw.Close()

		if err := server.Start(cmd.Context(), rw); err != nil {
			l.Fatal("LSP server failed", zap.Error(err))
		}
	},
}

func init() {
	LspCmd.PersistentFlags().StringVar(&lspCfgFile, "config", "", "config file (default is ./heddle-lsp.yaml)")
	LspCmd.Flags().String("log-path", "/tmp/heddle-lsp.log", "Path to log file")
	LspCmd.Flags().String("control-plane-addr", "localhost:50051", "Address of the Heddle Control Plane")
	viper.BindPFlag("log-path", LspCmd.Flags().Lookup("log-path"))
	viper.BindPFlag("control-plane-addr", LspCmd.Flags().Lookup("control-plane-addr"))
}
