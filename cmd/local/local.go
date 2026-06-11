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
)

// LocalCmd is the command to start the engine locally.
var LocalCmd = &cobra.Command{
	Use:   "local",
	Short: "Starts Control Plane and Worker",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Starting Heddle local services in foreground...")

		// Set up signal handling for graceful shutdown
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sigChan)

		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()

		if err := StartLocalServices(ctx); err != nil {
			logger.L().Error("Failed to start local services", logger.Error(err))
			return
		}

		fmt.Println("Heddle local services are running. Press Ctrl+C to stop...")

		select {
		case sig := <-sigChan:
			logger.L().Info("Received shutdown signal", logger.String("signal", sig.String()))
		case <-ctx.Done():
			logger.L().Info("Context cancelled, shutting down")
		}
	},
}

func StartLocalServices(ctx context.Context) error {
	defer logger.Sync()

	controlplaneSocket := runtime.ControlPlaneUDSPath
	workerSocket := runtime.WorkerUDSPath

	errCh := make(chan error, 2)

	// 1. Start Control Plane
	workerRegistry := registry.NewWorkerRegistry()
	cp := controlplane.NewControlPlaneServer(workerRegistry)
	go func() {
		defer logger.Sync()
		if err := cp.Listen(controlplaneSocket); err != nil {
			errCh <- fmt.Errorf("control plane failed: %w", err)
		}
	}()

	select {
	case <-cp.Ready:
		// CP is ready
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	// 2. Start Worker
	registry := locality.NewDataLocalityRegistry()
	nativePlugins := worker.NewNativePlugins()
	pluginServer := worker.NewPluginServer(registry, nativePlugins, workerSocket)
	worker, err := worker.NewWorker(pluginServer, controlplaneSocket)
	if err != nil {
		return fmt.Errorf("failed to create worker: %w", err)
	}
	go func() {
		if err := worker.Start(ctx); err != nil {
			errCh <- fmt.Errorf("worker failed: %w", err)
		}
	}()

	select {
	case <-worker.Ready:
		// Worker is ready
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}

	logger.L().Info("Heddle is running in local mode.")
	return nil
}
