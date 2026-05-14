package dev

import (
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/internal/services/lsp"
	"github.com/galgotech/heddle-lang/pkg/config"
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
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_LSP", lspCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		logger := zap.L()
		cpAddr, _ := cmd.Flags().GetString("control-plane-addr")
		server := lsp.NewServer(logger, cpAddr)

		rw := stdioRW{cmd.InOrStdin(), cmd.OutOrStdout()}
		defer rw.Close()

		if err := server.Start(cmd.Context(), rw); err != nil {
			logger.Fatal("LSP server failed", zap.Error(err))
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
