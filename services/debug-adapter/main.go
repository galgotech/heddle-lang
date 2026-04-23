package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"

	"github.com/google/go-dap"
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
	Use:   "heddle-dap",
	Short: "Heddle Debug Adapter",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		return initializeConfig()
	},
	Run: func(cmd *cobra.Command, args []string) {
		// Initialize logger with file output
		err := logger.Init(logger.Config{
			Development: true,
			OutputPaths: []string{"stdout", "heddle-dap.log"},
		})
		if err != nil {
			panic(err)
		}
		defer logger.Sync()

		logger.L().Info("Heddle Debug Adapter starting")

		if viper.GetBool("server") {
			startServer(viper.GetString("addr"))
		} else {
			serve(os.Stdin, os.Stdout)
		}
	},
}

func initializeConfig() error {
	return config.Init("HEDDLE_DAP", cfgFile)
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./heddle-dap.yaml)")

	rootCmd.Flags().Bool("server", false, "Start in server mode")
	rootCmd.Flags().String("addr", "localhost:4711", "Address to listen on in server mode")

	viper.BindPFlag("server", rootCmd.Flags().Lookup("server"))
	viper.BindPFlag("addr", rootCmd.Flags().Lookup("addr"))
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func startServer(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		logger.L().Fatal("Failed to listen", zap.Error(err))
	}
	logger.L().Info("Listening", zap.String("address", addr))
	for {
		conn, err := listener.Accept()
		if err != nil {
			logger.L().Error("Accept error", zap.Error(err))
			continue
		}
		go serve(conn, conn)
	}
}

func serve(r io.Reader, w io.Writer) {
	s := &session{
		reader:    bufio.NewReader(r),
		writer:    w,
		sendQueue: make(chan dap.Message),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go s.sendLoop(ctx)

	for {
		msg, err := dap.ReadProtocolMessage(s.reader)
		if err != nil {
			if err != io.EOF {
				logger.L().Error("Read error", zap.Error(err))
			}
			break
		}
		s.handleMessage(msg)
	}
}
