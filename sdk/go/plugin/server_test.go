package plugin_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
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
	schema := arrow.NewSchema([]arrow.Field{{Name: "res", Type: arrow.PrimitiveTypes.Int32}}, nil)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rb.Field(0).(*array.Int32Builder).Append(1)
	return core.NewTableFromRecord(rb.NewRecord()), nil
}

// Step without resource
func mockStateless(ctx context.Context, config StepConfigRoute, input *core.Table) (*core.Table, error) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "res", Type: arrow.PrimitiveTypes.Int32}}, nil)
	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rb.Field(0).(*array.Int32Builder).Append(2)
	return core.NewTableFromRecord(rb.NewRecord()), nil
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

func setupTestServer(t *testing.T) (flight.Client, func()) {
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
		}
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := flight.NewClientFromConn(conn, nil)

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
	body, _ := proto.Marshal(req)
	stream, err := client.DoAction(ctx, &flight.Action{Type: "init_resource", Body: body})
	require.NoError(t, err)

	res, err := stream.Recv()
	require.NoError(t, err)

	var resp pb.InitResourceResponse
	err = proto.Unmarshal(res.Body, &resp)
	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_SUCCESS, resp.Status)
	assert.NotEmpty(t, resp.ResourceId)
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
	body, _ := proto.Marshal(req)
	stream, err := client.DoAction(ctx, &flight.Action{Type: "init_resource", Body: body})
	require.NoError(t, err)

	res, err := stream.Recv()
	require.NoError(t, err)

	var resp pb.InitResourceResponse
	err = proto.Unmarshal(res.Body, &resp)
	require.NoError(t, err)
	assert.Equal(t, pb.StatusCode_BUSINESS_ERROR, resp.Status)
	assert.Contains(t, resp.ErrorMessage, "port cannot be 0")
}

func TestExecuteStep_WithResource_Success(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()

	// 1. Init Resource
	req := &pb.InitResourceRequest{
		ResourceName: "http_server",
		ConfigJson:   `{"host": "localhost", "port": 8080}`,
	}
	body, _ := proto.Marshal(req)
	stream, err := client.DoAction(ctx, &flight.Action{Type: "init_resource", Body: body})
	require.NoError(t, err)
	res, _ := stream.Recv()
	var initResp pb.InitResourceResponse
	proto.Unmarshal(res.Body, &initResp)
	require.Equal(t, pb.StatusCode_SUCCESS, initResp.Status)

	// 2. Execute Step
	execStream, err := client.DoExchange(ctx)
	require.NoError(t, err)

	execReq := &pb.ExecuteStepRequest{
		StepName:   "http_route",
		ResourceId: initResp.ResourceId,
		ConfigJson: `{"path": "/api/v1", "method": "GET"}`,
	}
	meta, _ := proto.Marshal(execReq)
	err = execStream.Send(&flight.FlightData{AppMetadata: meta})
	require.NoError(t, err)

	// Send dummy data
	schema := arrow.NewSchema([]arrow.Field{{Name: "dummy", Type: arrow.PrimitiveTypes.Int32}}, nil)
	writer := flight.NewRecordWriter(execStream, ipc.WithAllocator(memory.DefaultAllocator), ipc.WithSchema(schema))

	rb := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer rb.Release()
	rb.Field(0).(*array.Int32Builder).Append(1)
	rec := rb.NewRecord()
	defer rec.Release()

	require.NoError(t, writer.Write(rec))
	require.NoError(t, writer.Close())

	// Receive response
	reader, err := flight.NewRecordReader(execStream, ipc.WithAllocator(memory.DefaultAllocator))
	require.NoError(t, err)
	defer reader.Release()
	// In this mock, we return an empty record but successful status
	// (Our server implementation closes the stream after writing)
}

func TestDescribe(t *testing.T) {
	client, cleanup := setupTestServer(t)
	defer cleanup()

	ctx := context.Background()
	stream, err := client.DoAction(ctx, &flight.Action{Type: "describe"})
	require.NoError(t, err)

	res, err := stream.Recv()
	require.NoError(t, err)

	var resp pb.DescribeResponse
	err = proto.Unmarshal(res.Body, &resp)
	require.NoError(t, err)

	assert.Len(t, resp.Resources, 1)
	assert.Equal(t, "http_server", resp.Resources[0].Name)
}
