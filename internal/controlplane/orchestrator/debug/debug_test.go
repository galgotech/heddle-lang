package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/galgotech/heddle-lang/internal/controlplane/registry"
	"github.com/galgotech/heddle-lang/internal/models"
	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

func TestDebugOrchestrator_BasicStepping(t *testing.T) {
	reg := registry.NewNodeRegistry()
	orch := NewDebugOrchestrator(reg)

	// Create random TCP listener for server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	// Mock flight server to accept client & worker streams
	mockSrv := &mockFlightServer{registry: reg}
	flight.RegisterFlightServiceServer(srv, mockSrv)
	go srv.Serve(lis)
	defer srv.Stop()

	// Connect client
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Register worker & update capabilities before connecting stream
	reg.RegisterNode("worker-1", registry.NodeTypeWorker, models.WorkerRegistration{Address: "localhost:1234"})
	reg.UpdateCapabilities("worker-1", models.WorkerCapabilitiesUpdate{
		Capabilities: []string{"std.print"},
	})

	// 2. Worker Stream setup
	workerCtx := metadata.AppendToOutgoingContext(ctx, "worker-id", "worker-1")
	workerStream, err := client.DoExchange(workerCtx)
	require.NoError(t, err)

	// 3. Client Stream setup
	clientCtx := metadata.AppendToOutgoingContext(context.Background(), "client-id", "client-1")
	clientStream, err := client.DoExchange(clientCtx)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	// 4. Define a task
	stepID := "step-1"
	task := models.Task{
		ID:       "task-1",
		ClientID: "client-1",
		Program: ir.Program{
			Instructions: map[string]any{
				"flow-1": &ir.FlowInstruction{
					BaseInstruction: ir.BaseInstruction{ID: "flow-1"},
					Heads:           []string{stepID},
				},
				stepID: ir.StepInstruction{
					BaseInstruction: ir.BaseInstruction{
						ID:             stepID,
						SourceLocation: &ir.SourceLocation{Line: 5, Column: 3},
					},
					Call: []string{"std", "print"},
				},
			},
			Workflows: []string{"flow-1"},
		},
	}

	// 5. Run orchestrator
	go orch.OrchestrateTask(ctx, task)

	// 6. Client should receive starting workflow LOG message, then PAUSED message
	logMsg, err := clientStream.Recv()
	require.NoError(t, err)
	assert.Contains(t, string(logMsg.DataBody), "Starting debug execution")

	pausedMsg, err := clientStream.Recv()
	require.NoError(t, err)
	assert.Contains(t, string(pausedMsg.DataBody), "DEBUG_PAUSED:step-1:5:3")

	// 7. Client sends STEP command back
	err = clientStream.Send(&flight.FlightData{DataBody: []byte("STEP")})
	require.NoError(t, err)

	// 8. Worker receives step execution request
	workerData, err := workerStream.Recv()
	require.NoError(t, err)

	var execTask models.StepExecutionTask
	err = json.Unmarshal(workerData.DataBody, &execTask)
	require.NoError(t, err)
	assert.Equal(t, stepID, execTask.TaskID)

	// 9. Worker reports success
	res := models.TaskResult{
		TaskID:        execTask.TaskID,
		Status:        models.TaskStatusSuccess,
		OutputHandles: map[string]string{"out": "test-output-handle"},
	}
	resBody, _ := json.Marshal(res)
	err = workerStream.Send(&flight.FlightData{DataBody: resBody})
	require.NoError(t, err)

	// 10. Client receives STEP_COMPLETE message
	completeMsg, err := clientStream.Recv()
	require.NoError(t, err)
	assert.Contains(t, string(completeMsg.DataBody), "DEBUG_STEP_COMPLETE:step-1:SUCCESS")
}

type mockFlightServer struct {
	flight.BaseFlightServer
	registry *registry.NodeRegistry
}

func (s *mockFlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	ctx := stream.Context()
	meta, _ := metadata.FromIncomingContext(ctx)

	workerIDs := meta.Get("worker-id")
	clientIDs := meta.Get("client-id")

	var nodeID string
	if len(workerIDs) > 0 {
		nodeID = workerIDs[0]
	} else if len(clientIDs) > 0 {
		nodeID = clientIDs[0]
		s.registry.RegisterNode(nodeID, registry.NodeTypeClient, models.WorkerRegistration{})
	} else {
		return fmt.Errorf("unauthorized")
	}

	node, ok := s.registry.GetNode(nodeID)
	if ok {
		_ = node.ProcessStream(transport.NewExchangeServerStream(stream))
	}
	<-ctx.Done()
	return nil
}
