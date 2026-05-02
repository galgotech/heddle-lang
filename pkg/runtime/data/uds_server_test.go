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
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestUDSServer_FDPassing(t *testing.T) {
	socketPath := "/tmp/heddle-test.sock"
	_ = os.Remove(socketPath)

	manager := NewDataManager("/dev/shm/heddle-uds-test", 0)
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

	reader, err := ipc.NewReader(io.NewSectionReader(f, 0, fi.Size()))
	require.NoError(t, err)
	defer reader.Release()

	require.True(t, reader.Next())
	gotRec := reader.Record()
	assert.Equal(t, int64(3), gotRec.NumRows())
}
