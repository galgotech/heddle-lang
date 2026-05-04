package execution

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
)

func TestWorker_Delegation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start a Manual UDS Mock Plugin
	pluginAddr := "/tmp/heddle-test-plugin-go.sock"
	_ = os.Remove(pluginAddr)

	lis, err := net.Listen("unix", pluginAddr)
	require.NoError(t, err)
	defer os.Remove(pluginAddr)

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				unixConn := c.(*net.UnixConn)

				// Receive FD and Metadata
				fd, meta, err := data.RecvFDWithMetadata(unixConn)
				if err != nil {
					logger.L().Error("Mock plugin failed to receive FD", logger.Error(err))
					return
				}
				defer os.NewFile(uintptr(fd), "shm").Close()

				var req pb.ExecuteStepRequest
				if err := proto.Unmarshal(meta, &req); err != nil {
					logger.L().Error("Mock plugin failed to unmarshal request", logger.Error(err))
					return
				}

				// Respond with same handle (echo behavior)
				resp := &pb.ExecuteStepResponse{
					Status:       pb.StatusCode_SUCCESS,
					OutputHandle: req.InputHandle,
				}
				respData, _ := proto.Marshal(resp)
				_, _ = c.Write(respData)
			}(conn)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	// 2. Setup Worker
	w, err := NewWorker("worker-1", "localhost:9999")
	require.NoError(t, err)
	defer w.dataMgr.Cleanup()

	// Start Worker UDS server for FD passing
	go w.udsServer.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Create some input data in Worker's SHM
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
	rec := b.NewRecord()
	defer rec.Release()

	inputHandle := "input-1"
	err = w.dataMgr.Put(inputHandle, rec)
	require.NoError(t, err)

	// Register the plugin address in the worker's plugin manager
	_, err = w.pm.ConnectPlugin(ctx, "go", "unix://"+pluginAddr)
	require.NoError(t, err)

	// 3. Prepare task
	task := Task{
		ID: "task-1",
		Step: &ir.StepInstruction{
			Call: []string{"std:io", "echo"},
		},
		Tickets: map[string]*pb.FlightTicket{
			"default": {
				ResourceId: inputHandle,
				RouteType:  pb.RouteType_LOCAL,
			},
		},
	}

	// 4. Execute task
	outputHandle, err := w.executeTask(ctx, task)
	require.NoError(t, err)
	assert.NotEmpty(t, outputHandle)

	// 5. Verify output exists in DataManager
	gotRec, err := w.dataMgr.Get(outputHandle)
	require.NoError(t, err)
	defer gotRec.Release()
	assert.Equal(t, int64(2), gotRec.NumRows())
}
