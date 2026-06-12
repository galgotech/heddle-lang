package local

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	controlplane "github.com/galgotech/heddle-lang/internal/control-plane"
	"github.com/galgotech/heddle-lang/internal/control-plane/registry"
	"github.com/galgotech/heddle-lang/internal/worker"
	"github.com/galgotech/heddle-lang/pkg/logger"
	"github.com/galgotech/heddle-lang/pkg/runtime"
	"github.com/galgotech/heddle-lang/pkg/runtime/locality"
	"github.com/galgotech/heddle-lang/pkg/transport"
)

// LocalCmd is the command to start the engine locally.
var LocalCmd = &cobra.Command{
	Use:   "local",
	Short: "Starts Control Plane and Worker",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Starting Heddle local services in foreground...")
		defer logger.Sync()

		// Set up signal handling for graceful shutdown
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()

		if err := startLocalServices(ctx); err != nil {
			logger.L().Error("Failed to start local services", logger.Error(err))
			return
		}

		fmt.Println("Heddle local services are running. Press Ctrl+C to stop...")

		<-ctx.Done()
		logger.L().Info("Context cancelled, shutting down")
	},
}

func startLocalServices(ctx context.Context) error {
	workerSocket := runtime.WorkerUDSPath

	// 1. Start Control Plane
	workerRegistry := registry.NewWorkerRegistry()
	cp := controlplane.NewControlPlaneServer(workerRegistry)

	inMemory := transport.NewInMemory(cp)

	// 2. Start Worker
	registry := locality.NewDataLocalityRegistry()
	nativePlugins := worker.NewNativePlugins()
	pluginServer := worker.NewPluginServer(registry, nativePlugins, workerSocket)
	worker, err := worker.NewWorker(inMemory, pluginServer)
	if err != nil {
		return fmt.Errorf("failed to create worker: %w", err)
	}

	if err := worker.Start(ctx); err != nil {
		return fmt.Errorf("failed to start worker: %w", err)
	}

	logger.L().Info("Heddle is running ...")
	if err := inMemory.Start(ctx); err != nil {
		logger.L().Fatal("Failed to start local services", logger.Error(err))
	}

	return nil
}
