package local

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/galgotech/heddle-lang/internal/services/control-plane/server"
	heddleclient "github.com/galgotech/heddle-lang/pkg/client"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime/data"
	"github.com/galgotech/heddle-lang/pkg/runtime/execution"
	"github.com/galgotech/heddle-lang/pkg/runtime/transport"
	"github.com/galgotech/heddle-lang/pkg/stdlib"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

// LocalCmd is the root command for running Heddle in local standalone mode.
var LocalCmd = &cobra.Command{
	Use:   "local [file.he]",
	Short: "Run Heddle in local standalone mode",
	Long: `In local mode, Heddle starts an internal Control Plane and Worker using Unix Domain Sockets.
If a file is provided, it will be executed immediately.`,
	Args: cobra.MaximumNArgs(1),
	Run:  runStandalone,
}

func runStandalone(cmd *cobra.Command, args []string) {
	if err := logger.Init(logger.Config{Development: true}); err != nil {
		panic(err)
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.L().Info("Termination signal received, initiating shutdown...")
		cancel()
	}()

	// 1. Start Control Plane in background via Unix Domain Socket
	cpSocket := "/tmp/heddle-cp-standalone.sock"
	_ = os.Remove(cpSocket)
	cpLis, err := net.Listen("unix", cpSocket)
	if err != nil {
		logger.L().Fatal("failed to listen on CP socket", zap.Error(err))
	}
	defer os.Remove(cpSocket)

	go func() {
		logger.L().Info("Starting Control Plane (standalone mode)", zap.String("socket", cpSocket))
		server.Serve(cpLis)
	}()

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	// 2. Start Stdlib Plugin in background
	go runStdlibPlugin(ctx)

	// 3. Start Worker
	workerID := "standalone-worker"
	cpAddr := "unix://" + cpSocket

	conn, err := grpc.NewClient(cpAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		logger.L().Fatal("failed to connect to CP", zap.Error(err))
	}

	trans := transport.NewFlightTransport(conn)
	alloc := data.NewOSMemoryAllocator("/dev/shm/heddle")
	dataMgr := data.NewLocalMmapManager(alloc, 1<<30)

	worker := execution.NewWorker(workerID, trans, dataMgr, 10, 50*time.Millisecond)

	if err := worker.Register(ctx); err != nil {
		logger.L().Fatal("Failed to register worker", zap.Error(err))
	}

	// 4. Proactive Plugin Discovery
	go func() {
		// Wait a bit for the stdlib plugin to create its socket
		time.Sleep(200 * time.Millisecond)
		if err := worker.DiscoverPlugins(ctx); err != nil {
			logger.L().Warn("Plugin discovery failed", zap.Error(err))
		}
	}()

	go worker.StartHeartbeat(ctx)
	go worker.StartExecutionLoop(ctx)

	logger.L().Info("Heddle is running in standalone mode (Pure Local)",
		zap.String("workerID", workerID),
		zap.String("cp_socket", cpSocket))

	// 5. Submit workflow if file is provided
	if len(args) > 0 {
		filePath := args[0]
		go func() {
			// Give a bit more time for everything to be fully ready
			time.Sleep(500 * time.Millisecond)

			client, err := heddleclient.NewControlPlaneClient(cpAddr)
			if err != nil {
				logger.L().Error("Failed to create internal client", zap.Error(err))
				return
			}
			defer client.Close()

			content, err := os.ReadFile(filePath)
			if err != nil {
				logger.L().Error("Failed to read heddle file", zap.Error(err), zap.String("path", filePath))
				return
			}

			logger.L().Info("Submitting local workflow", zap.String("path", filePath))
			result, err := client.SubmitWorkflow(ctx, content)
			if err != nil {
				logger.L().Error("Workflow submission failed", zap.Error(err))
				return
			}
			fmt.Printf("\nWorkflow Submitted Successfully: %s\n", result)
		}()
	}

	<-ctx.Done()
	logger.L().Info("Heddle standalone shutdown complete")
}

func runStdlibPlugin(ctx context.Context) {
	namespace := "std"
	p := plugin.New(namespace)
	stdlib.RegisterAll(p)

	socketPath := fmt.Sprintf("/tmp/heddle-plugin-%s.sock", namespace)
	_ = os.Remove(socketPath)

	lis, err := net.Listen("unix", socketPath)
	if err != nil {
		logger.L().Error("failed to listen for stdlib plugin", zap.Error(err))
		return
	}
	defer os.Remove(socketPath)

	logger.L().Info("Stdlib plugin started", zap.String("namespace", namespace), zap.String("socket", socketPath))

	go func() {
		<-ctx.Done()
		lis.Close()
	}()

	if err := p.ServeListener(lis); err != nil {
		if ctx.Err() == nil {
			logger.L().Error("stdlib plugin server failed", zap.Error(err))
		}
	}
}
