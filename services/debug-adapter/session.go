package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"time"

	"github.com/google/go-dap"
	"go.uber.org/zap"

	"github.com/galgotech/heddle-lang/pkg/execution"
	"github.com/galgotech/heddle-lang/pkg/logger"
	heddlesdk "github.com/galgotech/heddle-lang/sdk/go"
)

type session struct {
	reader    *bufio.Reader
	writer    io.Writer
	sendQueue chan dap.Message
	seq       int

	cpClient *heddlesdk.ControlPlaneClient
	history  []execution.TaskUpdate
	curIndex int // Current index in history for Time-Travel
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
		// Initialize CP client
		addr := "localhost:50051" // Default CP addr
		client, err := heddlesdk.NewControlPlaneClient(addr)
		if err != nil {
			logger.L().Error("Failed to connect to CP", zap.Error(err))
		}
		s.cpClient = client

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

		// Send Stopped event to start the session
		s.sendStoppedEvent("entry")

	case *dap.StepBackRequest:
		if s.curIndex > 0 {
			s.curIndex--
		}
		s.send(&dap.StepBackResponse{
			Response: dap.Response{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "response",
				},
				RequestSeq: request.Seq,
				Success:    true,
				Command:    "stepBack",
			},
		})
		s.sendStoppedEvent("step")

	case *dap.NextRequest:
		s.syncHistory()
		if s.curIndex < len(s.history)-1 {
			s.curIndex++
		}
		s.send(&dap.NextResponse{
			Response: dap.Response{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "response",
				},
				RequestSeq: request.Seq,
				Success:    true,
				Command:    "next",
			},
		})
		s.sendStoppedEvent("step")

	case *dap.ScopesRequest:
		s.send(&dap.ScopesResponse{
			Response: dap.Response{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "response",
				},
				RequestSeq: request.Seq,
				Success:    true,
				Command:    "scopes",
			},
			Body: dap.ScopesResponseBody{
				Scopes: []dap.Scope{
					{
						Name:               "HeddleFrames",
						VariablesReference: 1, // Fixed ID for simplicity
						Expensive:          false,
					},
				},
			},
		})

	case *dap.VariablesRequest:
		vars := []dap.Variable{}
		s.syncHistory()

		if s.curIndex < len(s.history) {
			update := s.history[s.curIndex]
			vars = append(vars, dap.Variable{
				Name:  "CurrentStep",
				Value: update.TaskID,
			})
			vars = append(vars, dap.Variable{
				Name:  "Handle",
				Value: update.OutputHandle,
			})
			vars = append(vars, dap.Variable{
				Name:  "Status",
				Value: update.Status,
			})

			if update.Status == "completed" && update.OutputHandle != "" {
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				preview, _ := s.cpClient.GetHeddleFramePreview(ctx, update.OutputHandle)
				cancel()
				vars = append(vars, dap.Variable{
					Name:  "DataPreview",
					Value: preview,
				})
			}
		}

		s.send(&dap.VariablesResponse{
			Response: dap.Response{
				ProtocolMessage: dap.ProtocolMessage{
					Seq:  s.nextSeq(),
					Type: "response",
				},
				RequestSeq: request.Seq,
				Success:    true,
				Command:    "variables",
			},
			Body: dap.VariablesResponseBody{
				Variables: vars,
			},
		})

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

func (s *session) sendStoppedEvent(reason string) {
	s.send(&dap.StoppedEvent{
		Event: dap.Event{
			ProtocolMessage: dap.ProtocolMessage{
				Seq:  s.nextSeq(),
				Type: "event",
			},
			Event: "stopped",
		},
		Body: dap.StoppedEventBody{
			Reason:            reason,
			ThreadId:          1,
			AllThreadsStopped: true,
		},
	})
}

func (s *session) syncHistory() {
	if s.cpClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	history, err := s.cpClient.GetHistory(ctx)
	if err != nil {
		logger.L().Error("Failed to sync history", zap.Error(err))
		return
	}
	s.history = history
}
