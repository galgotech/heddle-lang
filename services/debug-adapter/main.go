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
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

var (
	isServer bool
	addr     string
)

var rootCmd = &cobra.Command{
	Use:   "heddle-dap",
	Short: "Heddle Debug Adapter",
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

		if isServer {
			startServer(addr)
		} else {
			serve(os.Stdin, os.Stdout)
		}
	},
}

func init() {
	rootCmd.Flags().BoolVar(&isServer, "server", false, "Start in server mode")
	rootCmd.Flags().StringVar(&addr, "addr", "localhost:4711", "Address to listen on in server mode")
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
