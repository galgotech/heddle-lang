package data

import (
	"context"
	"fmt"
	"net"
	"os"
	"syscall"

	"github.com/galgotech/heddle-lang/pkg/logger"
	"golang.org/x/sys/unix"
)

// UDSServer handles Unix Domain Socket connections for zero-copy FD passing.
type UDSServer struct {
	socketPath string
	manager    *DataManager
}

// NewUDSServer creates a new UDS server.
func NewUDSServer(socketPath string, manager *DataManager) *UDSServer {
	return &UDSServer{
		socketPath: socketPath,
		manager:    manager,
	}
}

// Start runs the UDS server.
func (s *UDSServer) Start(ctx context.Context) error {
	_ = os.Remove(s.socketPath)
	lc := net.ListenConfig{}
	l, err := lc.Listen(ctx, "unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on UDS: %w", err)
	}
	defer l.Close()
	defer os.Remove(s.socketPath)

	logger.L().Info("UDS server started", logger.String("path", s.socketPath))

	go func() {
		<-ctx.Done()
		l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				logger.L().Error("UDS accept error", logger.Error(err))
				continue
			}
		}

		go s.handleConnection(conn)
	}
}

func (s *UDSServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return
	}

	buf := make([]byte, 1024)
	n, err := unixConn.Read(buf)
	if err != nil {
		return
	}

	resourceID := string(buf[:n])
	file := s.manager.GetRegistry().GetFile(resourceID)
	if file == nil {
		_, _ = unixConn.Write([]byte("ERR: not found"))
		return
	}

	// Send FD via SCM_RIGHTS
	rights := unix.UnixRights(int(file.Fd()))
	_, _, err = unixConn.WriteMsgUnix([]byte("OK"), rights, nil)
	if err != nil {
		logger.L().Error("failed to send FD", logger.Error(err))
	}
}

// SendFD is a helper for low-level FD passing if needed outside the server.
func SendFD(conn *net.UnixConn, fd int) error {
	rights := unix.UnixRights(fd)
	_, _, err := conn.WriteMsgUnix([]byte("FD"), rights, nil)
	return err
}

// RecvFD is a helper for low-level FD receiving.
func RecvFD(conn *net.UnixConn) (int, error) {
	oob := make([]byte, unix.CmsgSpace(4))
	buf := make([]byte, 10)
	_, oobn, _, _, err := conn.ReadMsgUnix(buf, oob)
	if err != nil {
		return -1, err
	}

	msgs, err := syscall.ParseSocketControlMessage(oob[:oobn])
	if err != nil {
		return -1, err
	}

	fds, err := syscall.ParseUnixRights(&msgs[0])
	if err != nil {
		return -1, err
	}

	if len(fds) == 0 {
		return -1, fmt.Errorf("no FD received")
	}

	return fds[0], nil
}
