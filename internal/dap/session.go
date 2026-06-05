package dap

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/go-dap"

	"github.com/galgotech/heddle-lang/internal/client"
	"github.com/galgotech/heddle-lang/pkg/logger"
)

type TimetravelStep struct {
	StepID  string            `json:"step_id"`
	Status  string            `json:"status"`
	Outputs map[string]string `json:"outputs"`
}

type Session struct {
	logger logger.Logger
	rw     *bufio.ReadWriter
	sendMu sync.Mutex
	cpAddr string

	capabilities dap.Capabilities

	// Execution state
	programPath string
	workflow    string
	client      *client.ControlPlaneClient
	cancelExec  context.CancelFunc

	stateMu       sync.Mutex
	activeStepID  string
	activeLine    int
	activeCol     int
	activeInputs  map[string]string
	activeOutputs map[string]string
	history       []TimetravelStep
}

func (s *Session) serve(ctx context.Context) {
	for {
		msg, err := dap.ReadProtocolMessage(s.rw.Reader)
		if err != nil {
			if err != context.Canceled {
				s.logger.Debug("session closed", logger.Error(err))
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
	case *dap.NextRequest:
		s.onNext(req)
	case *dap.StepInRequest:
		s.onStepIn(req)
	case *dap.StepOutRequest:
		s.onStepOut(req)
	case *dap.ContinueRequest:
		s.onContinue(req)
	case *dap.StackTraceRequest:
		s.onStackTrace(req)
	case *dap.ScopesRequest:
		s.onScopes(req)
	case *dap.VariablesRequest:
		s.onVariables(req)
	default:
		if r, ok := msg.(dap.RequestMessage); ok {
			s.logger.Debug("unhandled request", logger.String("command", r.GetRequest().Command))
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

	s.logger.Info("launching debug session", logger.String("program", args.Program), logger.String("workflow", args.Workflow))
	s.programPath = args.Program
	s.workflow = args.Workflow

	// 1. Read program source
	content, err := os.ReadFile(args.Program)
	if err != nil {
		s.sendError(req.Request, fmt.Sprintf("Failed to read program file: %v", err))
		return
	}

	// 2. Connect to Heddle Control Plane
	execCtx, cancel := context.WithCancel(context.Background())
	s.cancelExec = cancel

	cPlaneClient, err := client.NewControlPlaneClient(execCtx, s.cpAddr)
	if err != nil {
		s.sendError(req.Request, fmt.Sprintf("Failed to connect to control plane at %s: %v", s.cpAddr, err))
		return
	}
	s.client = cPlaneClient

	// Respond Launch OK
	s.send(&dap.LaunchResponse{
		Response: *s.newResponse(req.Request),
	})

	// Send Output event to console
	s.send(&dap.OutputEvent{
		Event: *s.newEvent("output"),
		Body: dap.OutputEventBody{
			Output:   fmt.Sprintf("Heddle Debugger: Connected to Control Plane at %s. Submitting workflow '%s' with debug strategy...\n", s.cpAddr, args.Workflow),
			Category: "console",
		},
	})

	// 3. Start stream reader goroutine
	go s.runStreamReader(execCtx)

	// 4. Submit the workflow with "debug" strategy
	go func() {
		_, err := s.client.SubmitWorkflowDirect(string(content), args.Workflow, "debug")
		if err != nil {
			s.send(&dap.OutputEvent{
				Event: *s.newEvent("output"),
				Body: dap.OutputEventBody{
					Output:   fmt.Sprintf("Heddle Debugger: Execution failed: %v\n", err),
					Category: "stderr",
				},
			})
			s.send(&dap.TerminatedEvent{Event: *s.newEvent("terminated")})
		}
	}()
}

func (s *Session) runStreamReader(ctx context.Context) {
	stream := s.client.GetStream()
	if stream == nil {
		s.logger.Error("no client exchange stream found")
		return
	}

	for {
		rec, err := stream.Recv()
		if err != nil {
			s.logger.Info("debug stream closed or cancelled")
			s.send(&dap.TerminatedEvent{Event: *s.newEvent("terminated")})
			return
		}

		msg := string(rec.DataBody)
		if after, ok := strings.CutPrefix(msg, "LOG:"); ok {
			s.send(&dap.OutputEvent{
				Event: *s.newEvent("output"),
				Body: dap.OutputEventBody{
					Output:   after + "\n",
					Category: "console",
				},
			})
			if after == "Workflow completed successfully." {
				s.send(&dap.TerminatedEvent{Event: *s.newEvent("terminated")})
				return
			}
			if strings.HasPrefix(after, "Workflow failed:") {
				s.send(&dap.TerminatedEvent{Event: *s.newEvent("terminated")})
				return
			}
		} else if after, ok := strings.CutPrefix(msg, "DEBUG_PAUSED:"); ok {
			// Parse: DEBUG_PAUSED:step_id:line:col:inputs_json
			parts := strings.SplitN(after, ":", 4)
			if len(parts) >= 4 {
				stepID := parts[0]
				line, _ := strconv.Atoi(parts[1])
				col, _ := strconv.Atoi(parts[2])
				inputsJSON := parts[3]

				var inputs map[string]string
				_ = json.Unmarshal([]byte(inputsJSON), &inputs)

				s.stateMu.Lock()
				s.activeStepID = stepID
				s.activeLine = line
				s.activeCol = col
				s.activeInputs = inputs
				s.activeOutputs = nil // reset active outputs for current paused step
				s.stateMu.Unlock()

				s.send(&dap.StoppedEvent{
					Event: *s.newEvent("stopped"),
					Body: dap.StoppedEventBody{
						Reason:            "step",
						ThreadId:          1,
						AllThreadsStopped: true,
					},
				})
			}
		} else if after, ok := strings.CutPrefix(msg, "DEBUG_STEP_COMPLETE:"); ok {
			// Parse: DEBUG_STEP_COMPLETE:step_id:status:outputs_json
			parts := strings.SplitN(after, ":", 3)
			if len(parts) >= 3 {
				stepID := parts[0]
				status := parts[1]
				outputsJSON := parts[2]

				var outputs map[string]string
				_ = json.Unmarshal([]byte(outputsJSON), &outputs)

				s.stateMu.Lock()
				s.activeOutputs = outputs
				s.history = append(s.history, TimetravelStep{
					StepID:  stepID,
					Status:  status,
					Outputs: outputs,
				})
				s.stateMu.Unlock()
			}
		}
	}
}

func (s *Session) onDisconnect(req *dap.DisconnectRequest) {
	if s.client != nil && s.client.GetStream() != nil {
		_ = s.client.GetStream().Send(&flight.FlightData{DataBody: []byte("STOP")})
	}
	if s.cancelExec != nil {
		s.cancelExec()
	}

	s.send(&dap.DisconnectResponse{
		Response: *s.newResponse(req.Request),
	})
}

func (s *Session) onSetBreakpoints(req *dap.SetBreakpointsRequest) {
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

func (s *Session) onNext(req *dap.NextRequest) {
	if s.client != nil && s.client.GetStream() != nil {
		_ = s.client.GetStream().Send(&flight.FlightData{DataBody: []byte("STEP")})
	}
	s.send(&dap.NextResponse{
		Response: *s.newResponse(req.Request),
	})
}

func (s *Session) onStepIn(req *dap.StepInRequest) {
	if s.client != nil && s.client.GetStream() != nil {
		_ = s.client.GetStream().Send(&flight.FlightData{DataBody: []byte("STEP")})
	}
	s.send(&dap.StepInResponse{
		Response: *s.newResponse(req.Request),
	})
}

func (s *Session) onStepOut(req *dap.StepOutRequest) {
	if s.client != nil && s.client.GetStream() != nil {
		_ = s.client.GetStream().Send(&flight.FlightData{DataBody: []byte("STEP")})
	}
	s.send(&dap.StepOutResponse{
		Response: *s.newResponse(req.Request),
	})
}

func (s *Session) onContinue(req *dap.ContinueRequest) {
	if s.client != nil && s.client.GetStream() != nil {
		_ = s.client.GetStream().Send(&flight.FlightData{DataBody: []byte("STEP")})
	}
	s.send(&dap.ContinueResponse{
		Response: *s.newResponse(req.Request),
		Body: dap.ContinueResponseBody{
			AllThreadsContinued: true,
		},
	})
}

func (s *Session) onStackTrace(req *dap.StackTraceRequest) {
	s.stateMu.Lock()
	stepID := s.activeStepID
	line := s.activeLine
	col := s.activeCol
	s.stateMu.Unlock()

	frames := []dap.StackFrame{}
	if stepID != "" {
		frames = []dap.StackFrame{
			{
				Id:     1,
				Name:   "Step: " + stepID,
				Line:   line,
				Column: col,
				Source: &dap.Source{
					Name: filepath.Base(s.programPath),
					Path: s.programPath,
				},
			},
		}
	}

	s.send(&dap.StackTraceResponse{
		Response: *s.newResponse(req.Request),
		Body: dap.StackTraceResponseBody{
			StackFrames: frames,
			TotalFrames: len(frames),
		},
	})
}

func (s *Session) onScopes(req *dap.ScopesRequest) {
	s.send(&dap.ScopesResponse{
		Response: *s.newResponse(req.Request),
		Body: dap.ScopesResponseBody{
			Scopes: []dap.Scope{
				{Name: "Inputs", VariablesReference: 1000, Expensive: false},
				{Name: "Outputs", VariablesReference: 2000, Expensive: false},
				{Name: "Timetravel", VariablesReference: 3000, Expensive: false},
			},
		},
	})
}

func (s *Session) onVariables(req *dap.VariablesRequest) {
	vars := []dap.Variable{}
	ref := req.Arguments.VariablesReference

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if ref == 1000 {
		for k, v := range s.activeInputs {
			vars = append(vars, dap.Variable{
				Name:  k,
				Value: v,
				Type:  "HeddleFrame",
			})
		}
	} else if ref == 2000 {
		for k, v := range s.activeOutputs {
			vars = append(vars, dap.Variable{
				Name:  k,
				Value: v,
				Type:  "HeddleFrame",
			})
		}
	} else if ref == 3000 {
		for i, step := range s.history {
			outStr := ""
			for k, v := range step.Outputs {
				outStr += fmt.Sprintf("%s: %s\n", k, v)
			}
			if outStr == "" {
				outStr = "<void>"
			}
			vars = append(vars, dap.Variable{
				Name:  fmt.Sprintf("[%d] %s", i+1, step.StepID),
				Value: outStr,
				Type:  "StepHistory",
			})
		}
	}

	s.send(&dap.VariablesResponse{
		Response: *s.newResponse(req.Request),
		Body: dap.VariablesResponseBody{
			Variables: vars,
		},
	})
}

func (s *Session) send(msg dap.Message) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	err := dap.WriteProtocolMessage(s.rw.Writer, msg)
	if err != nil {
		s.logger.Error("failed to write message", logger.Error(err))
	}
	s.rw.Flush()
}

func (s *Session) sendError(req dap.Request, message string) {
	resp := s.newResponse(req)
	resp.Success = false
	s.send(&dap.ErrorResponse{
		Response: *resp,
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
