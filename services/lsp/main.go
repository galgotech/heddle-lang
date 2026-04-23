package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.lsp.dev/jsonrpc2"
	"go.lsp.dev/protocol"

	"github.com/galgotech/heddle-lang/pkg/config"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

const (
	lsName    = "heddle"
	lsVersion = "0.0.1"
)

var (
	cfgFile string
	state   *State
)

type stdioRW struct {
	io.Reader
	io.Writer
}

func (stdioRW) Close() error {
	return nil
}

var rootCmd = &cobra.Command{
	Use:   "heddle-lsp",
	Short: "Heddle Language Server",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig()
	},
	Run: func(cmd *cobra.Command, args []string) {
		logPath := viper.GetString("log-path")

		// Initialize shared logger with specific output path for LSP
		err := logger.Init(logger.Config{
			Development: true,
			OutputPaths: []string{logPath},
		})
		if err != nil {
			panic(err)
		}
		defer logger.Sync()

		state = NewState()

		ctx := context.Background()
		stream := jsonrpc2.NewStream(stdioRW{os.Stdin, os.Stdout})

		h := &lspHandler{
			client: nil, // Will be set after connection
			timers: make(map[protocol.DocumentURI]*time.Timer),
		}

		conn := jsonrpc2.NewConn(stream)
		h.client = protocol.ClientDispatcher(conn, logger.L())

		conn.Go(ctx, protocol.ServerHandler(h, jsonrpc2.MethodNotFoundHandler))

		logger.L().Info("Starting Heddle LSP server", logger.String("version", lsVersion))

		<-conn.Done()
	},
}

func initializeConfig() error {
	return config.Init("HEDDLE_LSP", cfgFile)
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-lsp.yaml)")

	rootCmd.Flags().String("log-path", "/tmp/heddle-lsp.log", "Path to log file")
	viper.BindPFlag("log-path", rootCmd.Flags().Lookup("log-path"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
