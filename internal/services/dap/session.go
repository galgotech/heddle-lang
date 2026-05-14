package dap

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/go-dap"
	"go.uber.org/zap"
)

type Session struct {
	logger *zap.Logger
	rw     *bufio.ReadWriter
	sendMu sync.Mutex
	cpAddr string

	capabilities dap.Capabilities
}

func (s *Session) serve(ctx context.Context) {
	for {
		msg, err := dap.ReadProtocolMessage(s.rw.Reader)
		if err != nil {
			if err != context.Canceled {
				s.logger.Debug("session closed", zap.Error(err))
			}
			return
		}

		s.handleMessage(ctx, msg)
	}
}

func (s *Session) handleMessage(ctx context.Context, msg dap.Message) {
	switch req := msg.(type) {
	case *dap.InitializeRequest:
		s.onInitialize(req)
	case *dap.LaunchRequest:
		s.onLaunch(req)
	case *dap.DisconnectRequest:
		s.onDisconnect(req)
	case *dap.SetBreakpointsRequest:
		s.onSetBreakpoints(req)
	case *dap.ConfigurationDoneRequest:
		s.onConfigurationDone(req)
	case *dap.ThreadsRequest:
		s.onThreads(req)
	default:
		if r, ok := msg.(dap.RequestMessage); ok {
			s.logger.Debug("unhandled request", zap.String("command", r.GetRequest().Command))
		}
	}
}

func (s *Session) onInitialize(req *dap.InitializeRequest) {
	s.capabilities = dap.Capabilities{
		SupportsConfigurationDoneRequest: true,
	}

	s.send(&dap.InitializeResponse{
		Response: *s.newResponse(req.Request),
		Body:     s.capabilities,
	})

	// After initialize response, we must send an initialized event
	s.send(&dap.InitializedEvent{
		Event: *s.newEvent("initialized"),
	})
}

func (s *Session) onLaunch(req *dap.LaunchRequest) {
	// Parse launch arguments
	var args struct {
		Program  string `json:"program"`
		Workflow string `json:"workflow"`
	}
	if err := json.Unmarshal(req.Arguments, &args); err != nil {
		s.sendError(req.Request, "Failed to parse launch arguments")
		return
	}

	s.logger.Info("launching debug session", zap.String("program", args.Program), zap.String("workflow", args.Workflow))

	// For now, just respond OK. In a real implementation, we would start the control plane or connect to it.
	s.send(&dap.LaunchResponse{
		Response: *s.newResponse(req.Request),
	})

	// Send Output event to show some progress in VS Code debug console
	s.send(&dap.OutputEvent{
		Event: *s.newEvent("output"),
		Body: dap.OutputEventBody{
			Output:   fmt.Sprintf("Heddle: Launching workflow '%s' in '%s'...\n", args.Workflow, args.Program),
			Category: "console",
		},
	})
}

func (s *Session) onDisconnect(req *dap.DisconnectRequest) {
	s.send(&dap.DisconnectResponse{
		Response: *s.newResponse(req.Request),
	})
}

func (s *Session) onSetBreakpoints(req *dap.SetBreakpointsRequest) {
	// Acknowledge breakpoints but don't do anything yet
	breakpoints := make([]dap.Breakpoint, len(req.Arguments.Breakpoints))
	for i, b := range req.Arguments.Breakpoints {
		breakpoints[i] = dap.Breakpoint{
			Verified: true,
			Line:     b.Line,
		}
	}

	s.send(&dap.SetBreakpointsResponse{
		Response: *s.newResponse(req.Request),
		Body: dap.SetBreakpointsResponseBody{
			Breakpoints: breakpoints,
		},
	})
}

func (s *Session) onConfigurationDone(req *dap.ConfigurationDoneRequest) {
	s.send(&dap.ConfigurationDoneResponse{
		Response: *s.newResponse(req.Request),
	})
}

func (s *Session) onThreads(req *dap.ThreadsRequest) {
	s.send(&dap.ThreadsResponse{
		Response: *s.newResponse(req.Request),
		Body: dap.ThreadsResponseBody{
			Threads: []dap.Thread{
				{Id: 1, Name: "main"},
			},
		},
	})
}

func (s *Session) send(msg dap.Message) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	err := dap.WriteProtocolMessage(s.rw.Writer, msg)
	if err != nil {
		s.logger.Error("failed to write message", zap.Error(err))
	}
	s.rw.Flush()
}

func (s *Session) sendError(req dap.Request, message string) {
	s.send(&dap.ErrorResponse{
		Response: *s.newResponse(req),
		Body: dap.ErrorResponseBody{
			Error: &dap.ErrorMessage{
				Format: message,
			},
		},
	})
}

func (s *Session) newResponse(req dap.Request) *dap.Response {
	return &dap.Response{
		ProtocolMessage: dap.ProtocolMessage{
			Seq:  0, // dap.WriteProtocolMessage will set this
			Type: "response",
		},
		Command:    req.Command,
		RequestSeq: req.Seq,
		Success:    true,
	}
}

func (s *Session) newEvent(event string) *dap.Event {
	return &dap.Event{
		ProtocolMessage: dap.ProtocolMessage{
			Seq:  0,
			Type: "event",
		},
		Event: event,
	}
}
