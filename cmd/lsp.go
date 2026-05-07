package main

import (
	"context"
	"io"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

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

var lspCmd = &cobra.Command{
	Use:   "lsp",
	Short: "Start the Heddle Language Server",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return config.Init("HEDDLE_LSP", lspCfgFile)
	},
	Run: func(cmd *cobra.Command, args []string) {
		logPath := viper.GetString("log-path")

		err := logger.Init(logger.Config{
			Development: true,
			OutputPaths: []string{logPath},
		})
		if err != nil {
			panic(err)
		}
		defer logger.Sync()

		state := lsp.NewState()
		ctx := context.Background()
		stream := jsonrpc2.NewStream(stdioRW{os.Stdin, os.Stdout})

		h := lsp.NewLSPHandler(state)
		conn := jsonrpc2.NewConn(stream)
		h.Client = protocol.ClientDispatcher(conn, logger.L())

		conn.Go(ctx, protocol.ServerHandler(h, jsonrpc2.MethodNotFoundHandler))

		logger.L().Info("Starting Heddle LSP server")
		<-conn.Done()
	},
}

func init() {
	lspCmd.PersistentFlags().StringVar(&lspCfgFile, "config", "", "config file (default is ./heddle-lsp.yaml)")
	lspCmd.Flags().String("log-path", "/tmp/heddle-lsp.log", "Path to log file")
	viper.BindPFlag("log-path", lspCmd.Flags().Lookup("log-path"))

	rootCmd.AddCommand(lspCmd)
}
