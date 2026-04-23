package main

import (
	"bufio"
	"context"
	"io"
	"net"
	"os"

	"github.com/google/go-dap"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/logger"
)

func main() {
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

	if len(os.Args) > 1 && os.Args[1] == "--server" {
		startServer("localhost:4711")
	} else {
		serve(os.Stdin, os.Stdout)
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
