package execution

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v18/arrow/flight"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/galgotech/heddle-lang/pkg/runtime/data"
)

// PluginInstance represents a running plugin process.
type PluginInstance struct {
	ID      string
	Cmd     *exec.Cmd
	Address string
	Client  flight.Client
	conn    *grpc.ClientConn
}

// PluginManager manages the lifecycle of multiple plugin processes.
type PluginManager struct {
	mu        sync.RWMutex
	plugins   map[string]*PluginInstance
	discovery chan string
}

// NewPluginManager creates a new plugin manager.
func NewPluginManager() *PluginManager {
	return &PluginManager{
		plugins:   make(map[string]*PluginInstance),
		discovery: make(chan string),
	}
}

// StartPlugin spawns a new plugin process and connects to its Flight server.
func (pm *PluginManager) StartPlugin(ctx context.Context, id string, command string, args ...string) (*PluginInstance, error) {
	cmd := exec.CommandContext(ctx, command, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("failed to get stdout pipe: %w", err)
	}

	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start plugin process: %w", err)
	}

	// Address discovery from stdout
	addressChan := make(chan string, 1)
	errChan := make(chan error, 1)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "ADDRESS=") {
				addressChan <- strings.TrimPrefix(line, "ADDRESS=")
				return
			}
		}
		if err := scanner.Err(); err != nil {
			errChan <- err
		}
		errChan <- fmt.Errorf("plugin process exited without reporting address")
	}()

	var address string
	select {
	case address = <-addressChan:
	case err := <-errChan:
		return nil, err
	case <-time.After(10 * time.Second):
		return nil, fmt.Errorf("timeout waiting for plugin address")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Connect to the plugin's Flight server
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to plugin: %w", err)
	}

	client := flight.NewClientFromConn(conn, nil)

	instance := &PluginInstance{
		ID:      id,
		Cmd:     cmd,
		Address: address,
		Client:  client,
		conn:    conn,
	}

	pm.mu.Lock()
	pm.plugins[id] = instance
	pm.mu.Unlock()

	return instance, nil
}

// ConnectPlugin connects to an existing plugin Flight server.
func (pm *PluginManager) ConnectPlugin(ctx context.Context, id string, address string) (*PluginInstance, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to plugin: %w", err)
	}

	client := flight.NewClientFromConn(conn, nil)

	instance := &PluginInstance{
		ID:      id,
		Address: address,
		Client:  client,
		conn:    conn,
	}

	pm.mu.Lock()
	pm.plugins[id] = instance
	pm.mu.Unlock()

	return instance, nil
}

// Describe retrieves metadata from the plugin.
func (pi *PluginInstance) Describe(ctx context.Context) (*pb.DescribeResponse, error) {
	action := &flight.Action{Type: "describe"}
	stream, err := pi.Client.DoAction(ctx, action)
	if err != nil {
		return nil, err
	}
	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	var desc pb.DescribeResponse
	if err := proto.Unmarshal(res.Body, &desc); err != nil {
		return nil, err
	}
	return &desc, nil
}

// InitResource initializes a stateful resource in the plugin.
func (pi *PluginInstance) InitResource(ctx context.Context, req *pb.InitResourceRequest) (*pb.InitResourceResponse, error) {
	body, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	action := &flight.Action{Type: "init_resource", Body: body}
	stream, err := pi.Client.DoAction(ctx, action)
	if err != nil {
		return nil, err
	}
	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	var resp pb.InitResourceResponse
	if err := proto.Unmarshal(res.Body, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ExecuteStep runs a step on the plugin via UDS/FD passing (zero-copy).
func (pi *PluginInstance) ExecuteStep(ctx context.Context, req *pb.ExecuteStepRequest, inputFd int) (*pb.ExecuteStepResponse, error) {
	// 1. Prepare UDS connection to plugin
	udsPath := strings.TrimPrefix(pi.Address, "unix://")
	conn, err := net.Dial("unix", udsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to dial plugin UDS: %w", err)
	}
	defer conn.Close()

	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, fmt.Errorf("connection is not a unix socket")
	}

	// 2. Marshal request for metadata
	meta, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// 3. Transmit FD and metadata
	if err := data.SendFDWithMetadata(unixConn, inputFd, meta); err != nil {
		return nil, fmt.Errorf("failed to transmit FD and metadata: %w", err)
	}

	// 4. Receive response from plugin
	respBuf := make([]byte, 4096)
	n, err := unixConn.Read(respBuf)
	if err != nil {
		return nil, fmt.Errorf("failed to read response from plugin: %w", err)
	}

	var resp pb.ExecuteStepResponse
	if err := proto.Unmarshal(respBuf[:n], &resp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &resp, nil
}

// StopPlugin stops a plugin process and cleans up resources.
func (pm *PluginManager) StopPlugin(id string) error {
	pm.mu.Lock()
	instance, ok := pm.plugins[id]
	if !ok {
		pm.mu.Unlock()
		return fmt.Errorf("plugin %s not found", id)
	}
	delete(pm.plugins, id)
	pm.mu.Unlock()

	if instance.conn != nil {
		instance.conn.Close()
	}

	if instance.Cmd != nil && instance.Cmd.Process != nil {
		return instance.Cmd.Process.Kill()
	}

	return nil
}

// GetPlugin retrieves a running plugin instance.
func (pm *PluginManager) GetPlugin(id string) (*PluginInstance, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	instance, ok := pm.plugins[id]
	return instance, ok
}
