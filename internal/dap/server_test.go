package dap

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/google/go-dap"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func TestDAPServer_Start(t *testing.T) {
	logger := zap.NewNop()
	addr := "localhost:0" // random port
	cpAddr := "localhost:50051"

	s := NewServer(logger, addr, cpAddr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Find a free port first to know where it will listen
	l, err := net.Listen("tcp", addr)
	assert.NoError(t, err)
	actualAddr := l.Addr().String()
	l.Close()

	s.addr = actualAddr

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Give it a moment to start
	time.Sleep(100 * time.Millisecond)

	// Try to connect
	conn, err := net.Dial("tcp", actualAddr)
	assert.NoError(t, err)
	if err == nil {
		conn.Close()
	}

	cancel()
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("server didn't shut down in time")
	}
}

func TestDAPSession_LaunchFailureOfflineControlPlane(t *testing.T) {
	// Create a temporary workflow file
	tmpFile, err := os.CreateTemp("", "dap_test_*.he")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("workflow hello {}")
	assert.NoError(t, err)
	tmpFile.Close()

	// Setup bufio ReadWriter with bytes.Buffers
	var readBuf bytes.Buffer
	var writeBuf bytes.Buffer
	rw := bufio.NewReadWriter(bufio.NewReader(&readBuf), bufio.NewWriter(&writeBuf))

	s := &Session{
		logger: zap.NewNop(),
		rw:     rw,
		cpAddr: "127.0.0.1:59999", // closed/offline port
	}

	// Prepare LaunchRequest
	req := &dap.LaunchRequest{
		Request: dap.Request{
			ProtocolMessage: dap.ProtocolMessage{
				Seq:  1,
				Type: "request",
			},
			Command: "launch",
		},
		Arguments: []byte(`{"program": "` + tmpFile.Name() + `", "workflow": "hello"}`),
	}

	s.onLaunch(req)

	// Print writeBuf content for debugging
	t.Logf("writeBuf length: %d, content: %q", writeBuf.Len(), writeBuf.String())

	// Verify that error response was written to writeBuf
	reader := bufio.NewReader(&writeBuf)
	resp, err := dap.ReadProtocolMessage(reader)
	if err != nil {
		t.Fatalf("ReadProtocolMessage failed: %v", err)
	}
	if resp == nil {
		t.Fatal("ReadProtocolMessage returned nil response")
	}

	errResp, ok := resp.(*dap.ErrorResponse)
	assert.True(t, ok)
	assert.False(t, errResp.Success)
	assert.Contains(t, errResp.Body.Error.Format, "Failed to connect to control plane")
}

func TestDAPSession_LaunchSuccess(t *testing.T) {
	// Create a temporary workflow file
	tmpFile, err := os.CreateTemp("", "dap_test_success_*.he")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	_, err = tmpFile.WriteString("workflow hello {}")
	assert.NoError(t, err)
	tmpFile.Close()

	// Start a mock flight server
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	assert.NoError(t, err)
	defer lis.Close()

	srv := grpc.NewServer()
	mockSrv := &mockDAPFlightServer{
		receivedCommand: make(chan string, 1),
	}
	flight.RegisterFlightServiceServer(srv, mockSrv)
	go srv.Serve(lis)
	defer srv.Stop()

	// Setup thread-safe net.Pipe for concurrent communication
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	s := &Session{
		logger: zap.NewNop(),
		rw:     bufio.NewReadWriter(bufio.NewReader(c1), bufio.NewWriter(c1)),
		cpAddr: lis.Addr().String(),
	}

	// Prepare LaunchRequest
	req := &dap.LaunchRequest{
		Request: dap.Request{
			ProtocolMessage: dap.ProtocolMessage{
				Seq:  1,
				Type: "request",
			},
			Command: "launch",
		},
		Arguments: []byte(`{"program": "` + tmpFile.Name() + `", "workflow": "hello"}`),
	}

	// Launch in a goroutine so onLaunch doesn't block if write buffer is full
	go s.onLaunch(req)

	// Verify that the launch response was successfully written
	reader := bufio.NewReader(c2)
	resp, err := dap.ReadProtocolMessage(reader)
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	launchResp, ok := resp.(*dap.LaunchResponse)
	assert.True(t, ok)
	assert.True(t, launchResp.Success)
}

type mockDAPFlightServer struct {
	flight.BaseFlightServer
	receivedCommand chan string
}

func (m *mockDAPFlightServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	return stream.Send(&flight.Result{Body: []byte("QUEUED:task-1")})
}

func (m *mockDAPFlightServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	// Simulate sending a LOG indicating completed workflow to trigger graceful exit in DAP stream reader
	err := stream.Send(&flight.FlightData{DataBody: []byte("LOG:Workflow completed successfully.")})
	if err != nil {
		return err
	}
	return nil
}
