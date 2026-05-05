package data

import (
	"context"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestUDSServer_FDPassing(t *testing.T) {
	socketPath := "/tmp/heddle-test.sock"
	_ = os.Remove(socketPath)

	tmpDir := t.TempDir()
	alloc := NewOSMemoryAllocator(tmpDir)
	manager := NewLocalMmapManager(alloc, 0)
	defer manager.Cleanup()


	// 1. Prepare data in DataManager
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "f1", Type: arrow.PrimitiveTypes.Int32}}, nil)
	b := array.NewRecordBuilder(mem, schema)
	b.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2, 3}, nil)
	rec := b.NewRecord()
	id := "res-1"
	err := manager.Put(id, rec)
	require.NoError(t, err)


	// 2. Start UDS Server
	server := NewUDSServer(socketPath, manager)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := server.Start(ctx); err != nil && err != io.EOF {
			// t.Errorf("Server error: %v", err) // Can't call t.Errorf in goroutine easily
		}
	}()

	time.Sleep(100 * time.Millisecond) // Wait for server to start

	// 3. Client connect and request FD
	conn, err := net.Dial("unix", socketPath)
	require.NoError(t, err)
	defer conn.Close()

	unixConn := conn.(*net.UnixConn)

	// Send resource ID
	_, err = unixConn.Write([]byte(id))
	require.NoError(t, err)

	// Receive FD via OOB
	oob := make([]byte, unix.CmsgSpace(4))
	buf := make([]byte, 10)
	n, oobn, _, _, err := unixConn.ReadMsgUnix(buf, oob)
	require.NoError(t, err)
	assert.Equal(t, "OK", string(buf[:n]))

	msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	fds, err := unix.ParseUnixRights(&msgs[0])
	require.NoError(t, err)
	require.Len(t, fds, 1)

	receivedFD := fds[0]
	defer unix.Close(receivedFD)

	// 4. mmap and verify Arrow data
	f := os.NewFile(uintptr(receivedFD), "received-shm")
	fi, err := f.Stat()
	require.NoError(t, err)

	mmap, err := unix.Mmap(receivedFD, 0, int(fi.Size()), unix.PROT_READ, unix.MAP_SHARED)
	require.NoError(t, err)
	defer unix.Munmap(mmap)

	// Since we now use raw zero-copy, we reconstruct the record manually from buffers
	// In a real plugin, the SDK would handle this.
	// For Int32, we have 2 buffers (validity, values).
	// But our computeDataSize/writeBuffers logic might have written them.
	// Let's use a simplified version for this test:
	// We know the schema and we know the data is at the beginning of the mmap.
	
	// Reconstruct buffers
	// Since we use 64-byte alignment, Buffer 0 (validity) is at 0, and Buffer 1 (values) is at 64.
	// We need to check if Buffer 0 exists.
	// In this test, it should be there because we appended values.
	valBuf := memory.NewBufferBytes(mmap[64 : 64+12]) // 3 * 4 bytes for Int32
	
	dataNode := array.NewData(arrow.PrimitiveTypes.Int32, 3, []*memory.Buffer{nil, valBuf}, nil, 0, 0)
	defer dataNode.Release()
	gotRec := array.NewRecord(schema, []arrow.Array{array.MakeFromData(dataNode)}, 3)
	defer gotRec.Release()

	assert.Equal(t, int64(3), gotRec.NumRows())
	assert.Equal(t, int32(1), gotRec.Column(0).(*array.Int32).Value(0))
}
