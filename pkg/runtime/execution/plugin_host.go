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

// PluginInstance represents an active polyglot plugin process. It encapsulates
// the process lifecycle state, communication handles (Arrow Flight), and
// the target address for locality-aware execution.
type PluginInstance struct {
	ID      string
	Cmd     *exec.Cmd
	Address string
	Client  flight.Client
	conn    *grpc.ClientConn
}

// PluginManager orchestrates the lifecycle of multiple polyglot plugin instances.
// It provides thread-safe registry operations and facilitates the discovery
// and connection to language-specific execution runtimes.
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

// GetPlugin retrieves a running plugin instance.
func (pm *PluginManager) GetPlugin(id string) (*PluginInstance, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	instance, ok := pm.plugins[id]
	return instance, ok
}

// StartPlugin spawns a new plugin process and establishes an Arrow Flight control
// channel. It monitors the process's stdout to discover its dynamic listen address.
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

	// Listen for the "ADDRESS=" signal on stdout to identify the plugin's gRPC/UDS endpoint.
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

	// Connect to the plugin's Arrow Flight server for control-plane operations.
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

// ConnectPlugin establishes an Arrow Flight connection to an already running plugin process.
func (pm *PluginManager) ConnectPlugin(ctx context.Context, id string, address string) (*PluginInstance, error) {
	// Connect to the plugin's control-plane endpoint.
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

// Describe retrieves the plugin's capabilities and step definitions using Flight DoAction.
func (pi *PluginInstance) Describe(ctx context.Context) (*pb.DescribeResponse, error) {
	action := &flight.Action{Type: "describe"}
	// Execute the metadata retrieval handshake.
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

// InitResource initializes a stateful resource (e.g., database connection) within the plugin.
func (pi *PluginInstance) InitResource(ctx context.Context, req *pb.InitResourceRequest) (*pb.InitResourceResponse, error) {
	body, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	// Transmit resource configuration via Flight DoAction.
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

// ExecuteStep runs a computational step using a high-performance zero-copy path.
// It transmits an input File Descriptor (FD) and task metadata to the plugin via UDS
// using SCM_RIGHTS, enabling the plugin to memory-map the data directly.
func (pi *PluginInstance) ExecuteStep(ctx context.Context, req *pb.ExecuteStepRequest, inputFd int) (*pb.ExecuteStepResponse, error) {
	// 1. Establish a raw Unix Domain Socket connection to the plugin host.
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

	// 2. Serialize the execution metadata for transmission.
	meta, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// 3. Transmit the input FD and metadata out-of-band using SCM_RIGHTS.
	if err := data.SendFDWithMetadata(unixConn, inputFd, meta); err != nil {
		return nil, fmt.Errorf("failed to transmit FD and metadata: %w", err)
	}

	// 4. Block until the plugin completes the task and returns a response.
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

// StopPlugin terminates a running plugin process and releases associated resources.
func (pm *PluginManager) StopPlugin(id string) error {
	pm.mu.Lock()
	instance, ok := pm.plugins[id]
	if !ok {
		pm.mu.Unlock()
		return fmt.Errorf("plugin %s not found", id)
	}
	delete(pm.plugins, id)
	pm.mu.Unlock()

	// Terminate the gRPC control-plane connection.
	if instance.conn != nil {
		instance.conn.Close()
	}

	// Forcefully kill the plugin process if it is still active.
	if instance.Cmd != nil && instance.Cmd.Process != nil {
		return instance.Cmd.Process.Kill()
	}

	return nil
}
