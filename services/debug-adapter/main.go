package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"os"

	"github.com/google/go-dap"
)

func main() {
	logFile, _ := os.OpenFile("heddle-dap.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	defer logFile.Close()
	log.SetOutput(logFile)

	log.Println("Heddle Debug Adapter starting...")

	if len(os.Args) > 1 && os.Args[1] == "--server" {
		startServer("localhost:4711")
	} else {
		serve(os.Stdin, os.Stdout)
	}
}

func startServer(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Listening on %s", addr)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
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
				log.Printf("Read error: %v", err)
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
				log.Printf("Write error: %v", err)
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
	log.Printf("Received: %T", msg)

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

		log.Println("Launch requested")

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
		log.Printf("Unhandled message type: %T", msg)
	}
}

func (s *session) nextSeq() int {
	s.seq++
	return s.seq
}
