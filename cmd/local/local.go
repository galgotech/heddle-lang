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

const pidFile = "/tmp/heddle.pid"

// LocalCmd is the group command for local engine management.
var LocalCmd = &cobra.Command{
	Use:   "local",
	Short: "Engine lifecycle management on the LOCAL MACHINE",
}

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts the local Control Plane and Worker",
	Run: func(cmd *cobra.Command, args []string) {
		daemon, _ := cmd.Flags().GetBool("daemon")
		if daemon {
			fmt.Println("Simulating starting local processes in background...")
			if err := os.WriteFile(pidFile, []byte("12345"), 0644); err != nil {
				fmt.Printf("Error writing PID file: %v\n", err)
				return
			}
			fmt.Printf("Mock: Heddle daemon started. PID: 12345 (saved to %s)\n", pidFile)
			return
		}

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

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Displays the current status and health of the daemon local",
	Run: func(cmd *cobra.Command, args []string) {
		if _, err := os.Stat(pidFile); err == nil {
			data, _ := os.ReadFile(pidFile)
			fmt.Printf("Mock: Healthy Control Plane and Worker\n")
			fmt.Printf("Status: RUNNING (PID: %s)\n", string(data))
			fmt.Println("Uptime: 2h 45m")
			fmt.Printf("Control Plane: %s\n", runtime.ControlPlaneUDSPath)
			fmt.Printf("Worker: %s\n", runtime.WorkerUDSPath)
		} else {
			fmt.Println("Status: STOPPED")
		}
	},
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Gracefully terminates local background processes",
	Run: func(cmd *cobra.Command, args []string) {
		if _, err := os.Stat(pidFile); err == nil {
			fmt.Println("Gracefully terminating local background processes...")
			os.Remove(pidFile)
			fmt.Println("Mock: Heddle services stopped.")
		} else {
			fmt.Println("No local processes are running in background.")
		}
	},
}

func init() {
	startCmd.Flags().Bool("daemon", false, "Runs background processes (detached from the terminal)")
	LocalCmd.AddCommand(startCmd)
	LocalCmd.AddCommand(statusCmd)
	LocalCmd.AddCommand(stopCmd)
}

func StartLocalServices(ctx context.Context) error {
	defer logger.Sync()

	cpSocket := runtime.ControlPlaneUDSPath
	workerSocket := runtime.WorkerUDSPath

	errCh := make(chan error, 2)

	// 1. Start Control Plane
	workerRegistry := registry.NewWorkerRegistry()
	cp := controlplane.NewControlPlaneServer(workerRegistry)
	go func() {
		defer logger.Sync()
		if err := cp.Listen(cpSocket); err != nil {
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
	nativePlugins := worker.NewNativePlugins(registry)
	pluginServer := worker.NewPluginServer(registry, nativePlugins, workerSocket)
	worker, err := worker.NewWorker(pluginServer, cpSocket)
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
