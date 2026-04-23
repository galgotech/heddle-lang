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

type session struct {
	reader    *bufio.Reader
	writer    io.Writer
	sendQueue chan dap.Message
	seq       int
}

func (s *session) sendLoop(ctx context.Context) {
	for {
		select {
		case msg := <-s.sendQueue:
			if err := dap.WriteProtocolMessage(s.writer, msg); err != nil {
				logger.L().Error("Write error", zap.Error(err))
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *session) send(msg dap.Message) {
	s.sendQueue <- msg
}

func (s *session) handleMessage(msg dap.Message) {
	logger.L().Debug("Received message", zap.String("type", string(os.Args[0]))) // Better way below

	switch request := msg.(type) {
	case *dap.InitializeRequest:
		s.send(&dap.InitializeResponse{
			Response: dap.Response{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "response",
				},
				RequestSeq: request.Seq,
				Success:    true,
				Command:    "initialize",
			},
			Body: dap.Capabilities{
				SupportsConfigurationDoneRequest: true,
				SupportsStepBack:                 true,
			},
		})

		s.send(&dap.InitializedEvent{
			Event: dap.Event{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "event",
				},
				Event: "initialized",
			},
		})

	case *dap.LaunchRequest:
		s.send(&dap.LaunchResponse{
			Response: dap.Response{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "response",
				},
				RequestSeq: request.Seq,
				Success:    true,
				Command:    "launch",
			},
		})

		logger.L().Info("Launch requested")

	case *dap.DisconnectRequest:
		s.send(&dap.DisconnectResponse{
			Response: dap.Response{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "response",
				},
				RequestSeq: request.Seq,
				Success:    true,
				Command:    "disconnect",
			},
		})

	default:
		logger.L().Warn("Unhandled message", zap.Any("msg", msg))
	}
}

func (s *session) nextSeq() int {
	s.seq++
	return s.seq
}
