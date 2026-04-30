package plugin_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
)

// -- Dummy Resource & Steps --

type ResourceConfigHttp struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type HttpResource struct {
	Host string
	Port int
	// ensure implements core.Resource implicitly if needed, or explicitly via Start
	Started bool
}

func (h *HttpResource) Start(ctx context.Context) error {
	h.Started = true
	return nil
}

func mockServer(ctx context.Context, config ResourceConfigHttp) (*HttpResource, error) {
	if config.Port == 0 {
		return nil, core.NewBusinessError("port cannot be 0")
	}
	return &HttpResource{Host: config.Host, Port: config.Port}, nil
}

type StepConfigRoute struct {
	Path   string `json:"path"`
	Method string `json:"method"`
}

// Normal step with resource
func mockRoute(ctx context.Context, config StepConfigRoute, res *HttpResource, input *core.Table) (*core.Table, error) {
	if !res.Started {
		return nil, core.NewBusinessError("resource not started")
	}
	return core.NewTableFromBytes([]byte(config.Path)), nil
}

// Step without resource
func mockStateless(ctx context.Context, config StepConfigRoute, input *core.Table) (*core.Table, error) {
	return core.NewTableFromBytes([]byte("stateless")), nil
}

// Step that panics
func mockPanicStep(ctx context.Context, config StepConfigRoute, input *core.Table) (*core.Table, error) {
	panic("unexpected failure")
}

// Step that returns business error
func mockBusinessErrStep(ctx context.Context, config StepConfigRoute, input *core.Table) (*core.Table, error) {
	return nil, core.NewBusinessError("validation failed")
}

// -- Test Helpers --

func setupTestServer(t *testing.T) (pb.PluginServiceClient, func()) {
	p := plugin.New()

	p.RegisterResource("http_server", mockServer)
	p.RegisterStep("http_route", mockRoute, plugin.WithResource("http_server"))
	p.RegisterStep("stateless_route", mockStateless)
	p.RegisterStep("panic_route", mockPanicStep)
	p.RegisterStep("business_err_route", mockBusinessErrStep)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		err := p.ServeListener(lis)
		if err != nil && err != grpc.ErrServerStopped {
			// Do not panic, net.Listener closed errors are common when test shuts down
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := pb.NewPluginServiceClient(conn)

	cleanup := func() {
		conn.Close()
		lis.Close()
	}

	return client, cleanup
}

// -- Tests --

func TestInitResource_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &pb.InitResourceRequest{
		ResourceName: "http_server",
		ConfigJson:   `{"host": "localhost", "port": 8080}`,
	}

	res, err := client.InitResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_SUCCESS, res.Status)
	assert.NotEmpty(t, res.ResourceId)
}

func TestInitResource_BusinessError(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &pb.InitResourceRequest{
		ResourceName: "http_server",
		ConfigJson:   `{"host": "localhost", "port": 0}`, // triggers error
	}

	res, err := client.InitResource(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_BUSINESS_ERROR, res.Status)
	assert.Contains(t, res.ErrorMessage, "port cannot be 0")
}

func TestExecuteStep_WithResource_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Init Resource
	initRes, err := client.InitResource(ctx, &pb.InitResourceRequest{
		ResourceName: "http_server",
		ConfigJson:   `{"host": "localhost", "port": 8080}`,
	})
	require.NoError(t, err)
	require.Equal(t, pb.StatusCode_SUCCESS, initRes.Status)

	// 2. Execute Step
	execRes, err := client.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		StepName:    "http_route",
		ResourceId:  initRes.ResourceId,
		ConfigJson:  `{"path": "/api/v1", "method": "GET"}`,
		InputTable:  []byte{},
	})
	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_SUCCESS, execRes.Status)
	assert.Equal(t, []byte("/api/v1"), execRes.OutputTable)
}

func TestExecuteStep_WithoutResource_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	execRes, err := client.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		StepName:    "stateless_route",
		ConfigJson:  `{"path": "/api/v2", "method": "POST"}`,
		InputTable:  []byte{},
	})
	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_SUCCESS, execRes.Status)
	assert.Equal(t, []byte("stateless"), execRes.OutputTable)
}

func TestExecuteStep_BusinessError(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	execRes, err := client.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		StepName: "business_err_route",
	})
	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_BUSINESS_ERROR, execRes.Status)
	assert.Contains(t, execRes.ErrorMessage, "validation failed")
}

func TestExecuteStep_PanicRecovery(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	execRes, err := client.ExecuteStep(ctx, &pb.ExecuteStepRequest{
		StepName: "panic_route",
	})

	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_FATAL_ERROR, execRes.Status)
	assert.Contains(t, execRes.ErrorMessage, "unexpected failure")
}
