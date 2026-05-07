package execution

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
	pb "github.com/galgotech/heddle-lang/sdk/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galgotech/heddle-lang/pkg/lang/compiler/ir"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
)


func TestWorker_NamespaceRouting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start two Real Plugin Servers with different namespaces
	tmpDir := t.TempDir()
	ns1Addr := filepath.Join(tmpDir, "heddle-plugin-ns1.sock")
	ns2Addr := filepath.Join(tmpDir, "heddle-plugin-ns2.sock")

	setupMock := func(addr string, ns string) {
		p := plugin.New(ns)
		p.RegisterStep("func", func(ctx context.Context, config struct{}, input core.Table) (core.Table, error) {
			return input, nil
		})

		lis, err := net.Listen("unix", addr)
		require.NoError(t, err)
		go p.ServeListener(lis)
	}

	setupMock(ns1Addr, "ns1")
	setupMock(ns2Addr, "ns2")
	defer os.Remove(ns1Addr)
	defer os.Remove(ns2Addr)
	time.Sleep(200 * time.Millisecond)

	// 2. Setup Worker
	shmPath := t.TempDir()
	os.Setenv("HEDDLE_SHM_PATH", shmPath)
	os.Setenv("HEDDLE_PLUGIN_SOCKET_DIR", tmpDir)
	defer os.Unsetenv("HEDDLE_SHM_PATH")
	defer os.Unsetenv("HEDDLE_PLUGIN_SOCKET_DIR")

	alloc := data.NewOSMemoryAllocator(shmPath)
	dataMgr := data.NewLocalMmapManager(alloc, 0)
	w := NewWorker("worker-ns", nil, dataMgr, 1, 0)
	defer w.dataMgr.Cleanup()



	// 3. Prepare data
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "f", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1}, nil)
	rec := b.NewRecord()
	w.dataMgr.Put("input", rec)
	rec.Release()

	// 4. Execute task for ns1
	task1 := Task{
		ID: "t1",
		Step: &ir.StepInstruction{
			Call: []string{"ns1", "func"},
		},
		Tickets: map[string]*pb.FlightTicket{
			"in": {ResourceId: "input", RouteType: pb.RouteType_LOCAL},
		},
	}

	out1, err := w.executeTask(ctx, task1)
	require.NoError(t, err, "Execution for ns1 should succeed")
	assert.NotEmpty(t, out1)
	
	// Verify data was processed (echoed back)
	rec1, err := w.dataMgr.Get(out1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rec1.NumRows())
	rec1.Release()

	// 5. Execute task for ns2
	task2 := Task{
		ID: "t2",
		Step: &ir.StepInstruction{
			Call: []string{"ns2", "func"},
		},
		Tickets: map[string]*pb.FlightTicket{
			"in": {ResourceId: "input", RouteType: pb.RouteType_LOCAL},
		},
	}
	out2, err := w.executeTask(ctx, task2)
	require.NoError(t, err, "Execution for ns2 should succeed")
	assert.NotEmpty(t, out2)

	// Verify data was processed (echoed back)
	rec2, err := w.dataMgr.Get(out2)
	require.NoError(t, err)
	assert.Equal(t, int64(1), rec2.NumRows())
	rec2.Release()
}
