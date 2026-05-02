package core

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/apache/arrow/go/v18/arrow/flight"
	"github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/galgotech/heddle-lang/sdk/go/proto"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ResolveTicket implements the Dual-Path logic to fetch data either via
// local shared memory (LOCAL) or over the network (REMOTE).
func ResolveTicket(ctx context.Context, ticket *proto.FlightTicket) (*Table, error) {
	switch ticket.RouteType {
	case proto.RouteType_LOCAL:
		return resolveLocal(ticket)
	case proto.RouteType_REMOTE:
		return resolveRemote(ctx, ticket)
	default:
		return nil, fmt.Errorf("unknown route type: %v", ticket.RouteType)
	}
}

func resolveLocal(ticket *proto.FlightTicket) (*Table, error) {
	addr := strings.TrimPrefix(ticket.Address, "unix://")

	conn, err := net.Dial("unix", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to worker UDS: %w", err)
	}
	defer conn.Close()

	// 1. Send ResourceID
	if _, err := conn.Write([]byte(ticket.ResourceId)); err != nil {
		return nil, fmt.Errorf("failed to send resource ID: %w", err)
	}

	// 2. Receive FD via SCM_RIGHTS
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return nil, fmt.Errorf("not a unix connection")
	}

	b := make([]byte, 1024)
	oob := make([]byte, unix.CmsgSpace(4)) // space for 1 int (FD)
	n, oobn, _, _, err := unixConn.ReadMsgUnix(b, oob)
	if err != nil {
		return nil, fmt.Errorf("failed to read message from UDS: %w", err)
	}

	if string(b[:n]) != "OK" {
		return nil, fmt.Errorf("worker returned error: %s", string(b[:n]))
	}

	msgs, err := unix.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return nil, fmt.Errorf("failed to parse OOB data: %w", err)
	}

	var fd int
	for _, msg := range msgs {
		fds, err := unix.ParseUnixRights(&msg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse unix rights: %w", err)
		}
		if len(fds) > 0 {
			fd = fds[0]
			break
		}
	}

	if fd == 0 {
		return nil, fmt.Errorf("no FD received from worker")
	}
	defer unix.Close(fd)

	// 3. mmap the FD and open Arrow data
	size, err := unix.Seek(fd, 0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to seek FD: %w", err)
	}

	data, err := unix.Mmap(fd, 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap FD: %w", err)
	}
	defer unix.Munmap(data)

	// 4. Create Arrow reader from mapped memory
	reader, err := ipc.NewFileReader(io.NewSectionReader(dummyReader(data), 0, size))
	if err != nil {
		// Try stream reader if file reader fails
		sReader, sErr := ipc.NewReader(io.NewSectionReader(dummyReader(data), 0, size))
		if sErr != nil {
			return nil, fmt.Errorf("failed to create arrow reader: %v (file) / %v (stream)", err, sErr)
		}
		defer sReader.Release()
		if !sReader.Next() {
			return nil, fmt.Errorf("empty stream")
		}
		rec := sReader.Record()
		return NewTableFromRecord(rec), nil
	}
	defer reader.Close()

	rec, err := reader.Record(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	return NewTableFromRecord(rec), nil
}

type dummyReader []byte

func (d dummyReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(d)) {
		return 0, io.EOF
	}
	n = copy(p, d[off:])
	return n, nil
}

func (d dummyReader) Read(p []byte) (n int, err error) {
	return copy(p, d), nil
}

func resolveRemote(ctx context.Context, ticket *proto.FlightTicket) (*Table, error) {
	addr := strings.TrimPrefix(ticket.Address, "grpc://")

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to peer %s: %w", addr, err)
	}
	defer conn.Close()

	client := flight.NewClientFromConn(conn, nil)
	stream, err := client.DoGet(ctx, &flight.Ticket{Ticket: []byte(ticket.ResourceId)})
	if err != nil {
		return nil, fmt.Errorf("DoGet failed for %s: %w", ticket.ResourceId, err)
	}

	reader, err := flight.NewRecordReader(stream)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	defer reader.Release()

	if !reader.Next() {
		return nil, fmt.Errorf("no data received from peer")
	}

	rec := reader.Record()
	return NewTableFromRecord(rec), nil
}
