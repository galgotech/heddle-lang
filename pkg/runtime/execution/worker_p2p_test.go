package execution

import (
	"context"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorker_P2PStreaming(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup Worker A (Producer)
	wA, err := NewWorker("worker-a", "localhost:9999") // CP doesn't need to exist for this test
	require.NoError(t, err)
	
	// Start Flight Server for Worker A
	addrA := "localhost:50051"
	go wA.StartFlightServer(ctx, addrA)
	time.Sleep(100 * time.Millisecond)

	// Produce data in Worker A's shm
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := b.NewRecord()
	resID := "shared-res-1"
	err = wA.dataMgr.Put(resID, rec)
	require.NoError(t, err)

	// 2. Setup Worker B (Consumer)
	wB, err := NewWorker("worker-b", "localhost:9999")
	require.NoError(t, err)

	// 3. Create a REMOTE ticket pointing to Worker A
	ticket := &proto.FlightTicket{
		RouteType:  proto.RouteType_REMOTE,
		Address:    "grpc://" + addrA,
		ResourceId: resID,
	}

	// 4. Fetch data from A to B
	localHandle, err := wB.fetchRemoteData(ctx, ticket)
	require.NoError(t, err)
	assert.NotEmpty(t, localHandle)

	// 5. Verify data is in Worker B's shm
	gotRec, err := wB.dataMgr.Get(localHandle)
	require.NoError(t, err)
	assert.Equal(t, int64(3), gotRec.NumRows())
}
